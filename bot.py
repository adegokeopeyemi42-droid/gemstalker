import os
import re
import time
import json
import asyncio
import logging
import threading
from dataclasses import dataclass, field
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler

import httpx
import websockets
from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# =========================================================
# CONFIG
# =========================================================

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID  = os.getenv("CHAT_ID")

PUMP_WS  = "wss://pumpportal.fun/api/data"

SOL_PRICE = 95.0  # updated live every 60s

# Solana CA pattern: base58, 32-44 chars
SOLANA_CA_RE = re.compile(r"[1-9A-HJ-NP-Za-km-z]{32,44}")

# =========================================================
# FILTERS  (relaxed for testing — tighten after confirming alerts work)
# =========================================================

MC_MIN           = 3_000    # USD
MC_MAX           = 500_000  # USD
MIN_SOL_IN       = 0.3      # SOL
MIN_BUYS         = 2        # total buys seen
MIN_BUYS_PER_MIN = 0.5      # per real 60s window

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)

# =========================================================
# DATA MODEL
# =========================================================

@dataclass
class Token:
    mint: str

    name:   str = "Unknown"
    symbol: str = "?"

    market_cap:    float = 0.0
    sol_in:        float = 0.0  # cumulative buy inflow
    bonding_curve: float = 0.0

    holders:    int   = 0
    top_holder: float = 0.0

    buy_count:  int = 0
    sell_count: int = 0

    buy_volume:  float = 0.0
    sell_volume: float = 0.0

    migrated: bool = False
    called:   bool = False

    created_at:  float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)

    # timestamps of buys in last 5 min for real bpm calc
    buys: deque = field(default_factory=lambda: deque(maxlen=500))


tokens: dict       = {}
recent_calls: deque = deque(maxlen=20)

# stats
event_counter = 0
buy_counter   = 0

# =========================================================
# LIVE SOL PRICE
# =========================================================

async def update_sol_price() -> None:
    global SOL_PRICE
    url = "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd"
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r    = await client.get(url)
                data = r.json()
                SOL_PRICE = float(data["solana"]["usd"])
                log.info(f"[PRICE] SOL updated: ${SOL_PRICE:.2f}")
        except Exception as e:
            log.warning(f"[PRICE] Fetch failed: {e} — keeping ${SOL_PRICE:.2f}")
        await asyncio.sleep(60)

# =========================================================
# HELPERS
# =========================================================

def fmt(n) -> str:
    n = float(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.2f}K"
    return f"{n:.2f}"


def buys_per_min(t: Token) -> float:
    """Real 60-second window."""
    cutoff = time.time() - 60
    return float(sum(1 for x in t.buys if x >= cutoff))


def buy_pressure(t: Token) -> int:
    total = t.buy_count + t.sell_count
    if total == 0:
        return 0
    return int((t.buy_count / total) * 100)


def token_age_str(t: Token) -> str:
    secs = int(time.time() - t.created_at)
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    return f"{secs // 3600}h"


def alpha_score(t: Token) -> float:
    score = 0.0
    score += min(t.sol_in * 0.4, 3)
    score += min(buys_per_min(t) * 0.3, 3)
    score += min(t.holders / 50, 2)
    if buy_pressure(t) > 70:
        score += 1
    if t.top_holder and t.top_holder < 15:
        score += 1
    return round(min(score, 10), 1)


def passes_filters(t: Token) -> tuple:
    """Returns (passes: bool, reason: str)"""
    if t.market_cap < MC_MIN:
        return False, f"MC too low (${fmt(t.market_cap)} < ${fmt(MC_MIN)})"
    if t.market_cap > MC_MAX:
        return False, f"MC too high (${fmt(t.market_cap)} > ${fmt(MC_MAX)})"
    if t.sol_in < MIN_SOL_IN:
        return False, f"Buy inflow too low ({t.sol_in:.3f} < {MIN_SOL_IN} SOL)"
    if t.buy_count < MIN_BUYS:
        return False, f"Buys too low ({t.buy_count} < {MIN_BUYS})"
    bpm = buys_per_min(t)
    if bpm < MIN_BUYS_PER_MIN:
        return False, f"Buys/min too low ({bpm:.2f} < {MIN_BUYS_PER_MIN})"
    return True, "OK"

# =========================================================
# TOKEN CLEANUP
# =========================================================

async def cleanup_tokens() -> None:
    """Remove tokens inactive for 45 minutes to prevent memory bloat."""
    while True:
        await asyncio.sleep(300)  # run every 5 minutes
        cutoff  = time.time() - 2700  # 45 minutes
        stale   = [m for m, t in tokens.items() if t.last_active < cutoff]
        for m in stale:
            del tokens[m]
        if stale:
            log.info(f"[CLEANUP] Removed {len(stale)} stale tokens. Tracking: {len(tokens)}")

# =========================================================
# EVENT STATS LOGGER
# =========================================================

async def log_stats() -> None:
    """Log events/min and buy count every 60s."""
    global event_counter, buy_counter
    while True:
        await asyncio.sleep(60)
        log.info(
            f"[STATS] Events last 60s: {event_counter} | "
            f"Buys: {buy_counter} | "
            f"Tokens tracked: {len(tokens)} | "
            f"Alerts sent: {sum(1 for t in tokens.values() if t.called)}"
        )
        event_counter = 0
        buy_counter   = 0

# =========================================================
# ALERT MESSAGE
# =========================================================

def build_alert(t: Token) -> str:
    pressure     = buy_pressure(t)
    net_flow     = t.buy_volume - t.sell_volume
    migration    = "Raydium" if t.migrated else "Bonding Curve"
    score        = alpha_score(t)
    holders_str  = str(t.holders) if t.holders else "N/A"
    top_str      = f"{t.top_holder:.1f}%" if t.top_holder else "N/A"
    bpm          = buys_per_min(t)

    return (
        "🚨 EARLY GEM DETECTED 🚨\n\n"
        f"🪙 Token: {t.name} ({t.symbol})\n"
        f"⏱ Age: {token_age_str(t)}\n\n"
        f"💰 Market Cap:     ${fmt(t.market_cap)}\n"
        f"💧 Buy Inflow:     {t.sol_in:.3f} SOL\n"
        f"🔄 Net Buy Flow:   {net_flow:.3f} SOL\n"
        f"📊 Total Volume:   {t.buy_volume + t.sell_volume:.3f} SOL\n"
        f"👥 Holders:        {holders_str}\n"
        f"📈 Buy Pressure:   {pressure}%\n"
        f"⚡ Buys/Sells:     {t.buy_count} / {t.sell_count}\n"
        f"🔥 Buys/Min:       {bpm:.1f}\n"
        f"🏆 Top Holder:     {top_str}\n"
        f"🚀 Status:         {migration}\n"
        f"⭐ Alpha Score:    {score}/10\n\n"
        f"💵 SOL:            ${SOL_PRICE:.2f}\n\n"
        "━━━━━━━━━━━━━━━\n"
        f"📍 Contract:\n{t.mint}\n"
        "━━━━━━━━━━━━━━━"
    )

# =========================================================
# SEND ALERT
# =========================================================

async def send_alert(app: Application, t: Token) -> None:
    photon = f"https://photon-sol.tinyastro.io/en/lp/{t.mint}"
    bullx  = f"https://bullx.io/terminal?chainId=1399811149&address={t.mint}"
    dex    = f"https://dexscreener.com/solana/{t.mint}"

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("⚡ Photon",      url=photon),
            InlineKeyboardButton("🐂 BullX",       url=bullx),
        ],
        [
            InlineKeyboardButton("📊 DexScreener", url=dex),
        ],
    ])

    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=build_alert(t),
            disable_web_page_preview=True,
            reply_markup=keyboard,
        )
        recent_calls.append({
            "name":       t.name,
            "symbol":     t.symbol,
            "mint":       t.mint,
            "market_cap": t.market_cap,
            "score":      alpha_score(t),
            "time":       time.time(),
        })
        log.info(f"[ALERT SENT] {t.name} ({t.symbol}) | MC=${fmt(t.market_cap)}")
    except Exception as e:
        log.error(f"[ALERT ERROR] Failed to send: {e}")

# =========================================================
# CA ANALYZER
# =========================================================

async def fetch_dexscreener(ca: str) -> dict | None:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r    = await client.get(url)
            data = r.json()
            pairs = data.get("pairs")
            if pairs:
                # pick Solana pair with highest liquidity
                sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
                if sol_pairs:
                    return max(sol_pairs, key=lambda p: float(p.get("liquidity", {}).get("usd", 0)))
    except Exception as e:
        log.warning(f"[DEX] DexScreener failed for {ca}: {e}")
    return None


def analyze_pair(ca: str, pair: dict) -> str:
    base    = pair.get("baseToken", {})
    name    = base.get("name", "Unknown")
    symbol  = base.get("symbol", "?")

    mc      = float(pair.get("marketCap", 0) or pair.get("fdv", 0) or 0)
    liq     = float((pair.get("liquidity") or {}).get("usd", 0))
    vol24   = float((pair.get("volume") or {}).get("h24", 0))
    vol1    = float((pair.get("volume") or {}).get("h1", 0))
    buys1   = int((pair.get("txns") or {}).get("h1", {}).get("buys", 0))
    sells1  = int((pair.get("txns") or {}).get("h1", {}).get("sells", 0))
    buys24  = int((pair.get("txns") or {}).get("h24", {}).get("buys", 0))
    sells24 = int((pair.get("txns") or {}).get("h24", {}).get("sells", 0))
    price_change_1h  = float((pair.get("priceChange") or {}).get("h1", 0) or 0)
    price_change_24h = float((pair.get("priceChange") or {}).get("h24", 0) or 0)

    total_txns = buys1 + sells1
    pressure   = int((buys1 / total_txns) * 100) if total_txns else 0

    dex   = f"https://dexscreener.com/solana/{ca}"
    photon = f"https://photon-sol.tinyastro.io/en/lp/{ca}"
    bullx = f"https://bullx.io/terminal?chainId=1399811149&address={ca}"

    # ---- rug / safety checks ----
    flags = []
    if liq < 5000:
        flags.append("⚠️ Liquidity very low")
    if vol24 < 1000:
        flags.append("⚠️ Volume dead (<$1K/24h)")
    if sells1 > buys1 * 2:
        flags.append("⚠️ Heavy sell pressure")
    if buys1 == 0 and sells1 == 0:
        flags.append("⚠️ No activity in last hour")
    if mc > 0 and liq > 0 and (liq / mc) < 0.02:
        flags.append("⚠️ Thin liquidity vs MC")

    # ---- rating ----
    red_flags = len(flags)
    if red_flags == 0 and pressure > 65 and vol1 > 5000:
        rating = "✅ RUNNER POTENTIAL"
    elif red_flags >= 3 or (sells1 > buys1 * 2):
        rating = "❌ AVOID"
    else:
        rating = "⚠️ HIGH RISK"

    # ---- AI read ----
    reads = []
    if pressure > 70:
        reads.append("Momentum strong")
    elif pressure < 40:
        reads.append("Sell pressure dominant")
    if price_change_1h > 20:
        reads.append("Pumping hard in last hour")
    elif price_change_1h < -20:
        reads.append("Dumping in last hour")
    if mc < 50_000:
        reads.append("Early stage — high risk, high reward")
    elif mc < 500_000:
        reads.append("Mid cap — still early")
    if liq < 10_000:
        reads.append("Low liquidity — slippage risk")
    if not reads:
        reads.append("No strong signal detected")

    flags_str = "\n".join(flags) if flags else "No major red flags"
    reads_str = "\n  ".join(reads)

    vol_label = "High" if vol24 > 50_000 else "Medium" if vol24 > 5_000 else "Low"

    return (
        f"🪙 {name} ({symbol})\n\n"
        f"💰 MC: ${fmt(mc)}\n"
        f"💧 Liquidity: ${fmt(liq)}\n"
        f"📈 Volume 1h: ${fmt(vol1)} | 24h: ${fmt(vol24)} ({vol_label})\n"
        f"📊 Buys/Sells 1h: {buys1} / {sells1}\n"
        f"🔥 Buy Pressure: {pressure}%\n"
        f"📉 Price Change: 1h {price_change_1h:+.1f}% | 24h {price_change_24h:+.1f}%\n\n"
        f"Safety Check:\n{flags_str}\n\n"
        f"AI Read:\n  {reads_str}\n\n"
        f"Rating: {rating}\n\n"
        f"🔗 Dex: {dex}\n"
        f"⚡ Photon: {photon}\n"
        f"🐂 BullX: {bullx}"
    )


async def analyze_ca(ca: str) -> str:
    log.info(f"[CA] Analyzing: {ca}")
    pair = await fetch_dexscreener(ca)
    if pair:
        return analyze_pair(ca, pair)
    return (
        f"Could not find data for:\n{ca}\n\n"
        "Token may be too new or not yet listed on DexScreener.\n"
        f"Try manually: https://dexscreener.com/solana/{ca}"
    )

# =========================================================
# COMMAND HANDLERS
# =========================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 GemStalker is live!\n\n"
        "Scanning pump.fun in real-time.\n"
        "Alerts fire here when a gem passes filters.\n\n"
        "Commands:\n"
        "  /start    - This message\n"
        "  /status   - Live tracking stats\n"
        "  /calls    - Last 20 alerts\n"
        "  /filters  - Current filter settings\n"
        "  /analyze  - Analyze a Solana CA\n\n"
        "Tip: Paste any Solana CA directly and I will analyze it instantly."
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    total    = len(tokens)
    alerted  = sum(1 for t in tokens.values() if t.called)
    watching = total - alerted

    candidates = sorted(
        [t for t in tokens.values() if not t.called and t.buy_count > 0],
        key=lambda t: t.sol_in,
        reverse=True
    )[:3]

    msg = (
        "📡 GemStalker Status\n\n"
        f"  Tokens tracked:  {total}\n"
        f"  Alerts sent:     {alerted}\n"
        f"  Still watching:  {watching}\n"
        f"  SOL Price:       ${SOL_PRICE:.2f} (live)\n"
    )

    if candidates:
        msg += "\nTop candidates:\n"
        for t in candidates:
            passed, reason = passes_filters(t)
            bpm = buys_per_min(t)
            msg += (
                f"  {t.name} ({t.symbol})\n"
                f"    MC:${fmt(t.market_cap)} | Inflow:{t.sol_in:.2f} SOL"
                f" | Buys:{t.buy_count} | B/min:{bpm:.1f}\n"
                f"    Blocking: {reason}\n"
            )

    await update.message.reply_text(msg)


async def cmd_calls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_calls:
        await update.message.reply_text(
            "No calls yet. Watching the market...\n"
            "Use /status to see top candidates."
        )
        return

    lines = ["🔥 Recent Calls:\n"]
    for i, c in enumerate(reversed(recent_calls), 1):
        age_min = int((time.time() - c["time"]) / 60)
        age_str = f"{age_min}m ago" if age_min < 60 else f"{age_min // 60}h ago"
        lines.append(
            f"{i}. {c['name']} ({c['symbol']})\n"
            f"   MC: ${fmt(c['market_cap'])} | Score: {c['score']}/10 | {age_str}\n"
            f"   {c['mint'][:20]}...\n"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "⚙️ Current Filters:\n\n"
        f"  Market Cap:      ${fmt(MC_MIN)} - ${fmt(MC_MAX)}\n"
        f"  Min Buy Inflow:  {MIN_SOL_IN} SOL\n"
        f"  Min Buys:        {MIN_BUYS}\n"
        f"  Min Buys/Min:    {MIN_BUYS_PER_MIN}\n\n"
        f"  SOL Price:       ${SOL_PRICE:.2f} (live)\n\n"
        "Holders/top holder not filtered\n"
        "(pump.fun rarely sends that data in live trade events)"
    )


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: /analyze <contract_address>\nExample: /analyze EPjFWdd5..."
        )
        return

    ca = args[0].strip()
    if not SOLANA_CA_RE.fullmatch(ca):
        await update.message.reply_text("That doesn't look like a valid Solana CA.")
        return

    msg = await update.message.reply_text("🔍 Analyzing...")
    result = await analyze_ca(ca)
    await msg.edit_text(result, disable_web_page_preview=True)


async def handle_ca_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Auto-detect Solana CA pasted directly into chat."""
    text = (update.message.text or "").strip()
    match = SOLANA_CA_RE.fullmatch(text)
    if not match:
        # also try to find a CA embedded in a longer message
        matches = SOLANA_CA_RE.findall(text)
        if not matches:
            return
        ca = matches[0]
    else:
        ca = text

    msg = await update.message.reply_text(f"🔍 Analyzing {ca[:12]}...")
    result = await analyze_ca(ca)
    await msg.edit_text(result, disable_web_page_preview=True)

# =========================================================
# EVENT HANDLER
# =========================================================

async def handle_event(app: Application, msg: dict) -> None:
    global event_counter, buy_counter

    tx_type = msg.get("txType")
    mint    = msg.get("mint")

    event_counter += 1

    if not mint:
        return

    # log raw incoming buys/sells at DEBUG level so logs don't flood
    if tx_type in ("buy", "sell"):
        log.debug(
            f"[{tx_type.upper()}] mint={mint[:8]}.. "
            f"mcSol={msg.get('marketCapSol', '?')} "
            f"sol={msg.get('solAmount', '?')}"
        )

    if mint not in tokens:
        tokens[mint] = Token(mint=mint)

    t = tokens[mint]
    t.last_active = time.time()

    t.name   = msg.get("name",   t.name)
    t.symbol = msg.get("symbol", t.symbol)

    market_cap_sol = float(msg.get("marketCapSol", 0) or 0)
    t.market_cap   = market_cap_sol * SOL_PRICE

    sol_amount = float(msg.get("solAmount", 0) or 0)

    if tx_type == "buy":
        buy_counter  += 1
        t.buy_count  += 1
        t.buy_volume += sol_amount
        t.sol_in     += sol_amount
        t.buys.append(time.time())

    elif tx_type == "sell":
        t.sell_count  += 1
        t.sell_volume += sol_amount

    t.bonding_curve = float(msg.get("bondingCurveProgress", 0) or 0)

    if msg.get("raydiumPool"):
        t.migrated = True

    holder_count = int(msg.get("holderCount", 0) or 0)
    if holder_count:
        t.holders = max(t.holders, holder_count)

    top_holder = float(msg.get("topHolder", 0) or 0)
    if top_holder:
        t.top_holder = top_holder

    if not t.called:
        passed, reason = passes_filters(t)
        if passed:
            t.called = True
            log.info(f"[PASS] {t.name} ({t.symbol}) MC=${fmt(t.market_cap)}")
            await send_alert(app, t)
        elif tx_type == "buy" and t.buy_count % 5 == 0:
            # log filter failure every 5 buys so we can see what's blocking
            log.info(
                f"[FAIL] {t.name} ({t.symbol}) | "
                f"MC=${fmt(t.market_cap)} | Inflow={t.sol_in:.3f} | "
                f"Buys={t.buy_count} | B/min={buys_per_min(t):.2f} | "
                f"Reason: {reason}"
            )

# =========================================================
# WEBSOCKET LOOP
# =========================================================

async def websocket_loop(app: Application) -> None:
    reconnect_count = 0
    while True:
        try:
            log.info(f"[WS] Connecting... (attempt {reconnect_count + 1})")
            async with websockets.connect(
                PUMP_WS,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=10,
            ) as ws:
                reconnect_count = 0
                log.info("[WS] Connected to pumpportal.fun")

                # Subscribe to new tokens AND all token trades
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("[WS] Subscribed: subscribeNewToken")

                await ws.send(json.dumps({"method": "subscribeTokenTrade"}))
                log.info("[WS] Subscribed: subscribeTokenTrade")

                heartbeat = time.time()
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        await handle_event(app, msg)

                        # heartbeat log every 30s to confirm stream is alive
                        if time.time() - heartbeat > 30:
                            heartbeat = time.time()
                            log.info(
                                f"[WS HEARTBEAT] Stream alive | "
                                f"Events: {event_counter} | Tokens: {len(tokens)}"
                            )
                    except Exception as e:
                        log.error(f"[WS] Event parse error: {e}")

        except Exception as e:
            reconnect_count += 1
            wait = min(5 * reconnect_count, 30)
            log.error(f"[WS] Disconnected: {e} — reconnecting in {wait}s")
            await asyncio.sleep(wait)

# =========================================================
# STARTUP HOOK
# =========================================================

async def post_init(app: Application) -> None:
    log.info("[INIT] Starting background tasks...")
    asyncio.create_task(update_sol_price())
    asyncio.create_task(websocket_loop(app))
    asyncio.create_task(cleanup_tokens())
    asyncio.create_task(log_stats())
    log.info("[INIT] All tasks started")

# =========================================================
# HEALTH SERVER
# =========================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK - GemStalker running")

    def log_message(self, format, *args):
        pass


def run_health_server() -> None:
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info(f"[HEALTH] Server on port {port}")
    server.serve_forever()

# =========================================================
# MAIN
# =========================================================

def main() -> None:
    if not TG_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN env var is not set")
    if not CHAT_ID:
        raise RuntimeError("CHAT_ID env var is not set")

    threading.Thread(target=run_health_server, daemon=True).start()

    app = Application.builder().token(TG_TOKEN).build()

    app.add_handler(CommandHandler("start",   cmd_start))
    app.add_handler(CommandHandler("status",  cmd_status))
    app.add_handler(CommandHandler("calls",   cmd_calls))
    app.add_handler(CommandHandler("filters", cmd_filters))
    app.add_handler(CommandHandler("analyze", cmd_analyze))

    # catch any plain text message that looks like a CA
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_ca_message))

    app.post_init = post_init

    log.info("[MAIN] GemStalker starting...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
