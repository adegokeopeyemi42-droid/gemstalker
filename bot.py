import os
import time
import json
import asyncio
import logging
import threading
from dataclasses import dataclass, field
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler

import websockets
from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import Application, CommandHandler, ContextTypes

# =========================================================
# CONFIG
# =========================================================

TG_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("CHAT_ID")

PUMP_WS   = "wss://pumpportal.fun/api/data"
SOL_PRICE = 150

# =========================================================
# FILTERS (relaxed so you actually get alerts)
# =========================================================

MC_MIN           = 5_000   # min market cap USD
MC_MAX           = 100_000 # max market cap USD
MIN_SOL_IN       = 2       # min SOL bought in total
MIN_HOLDERS      = 10      # min unique holders
MIN_BUYS_PER_MIN = 3       # min buys per minute
MAX_TOP_HOLDER   = 30      # max % held by top wallet

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
    sol_in:        float = 0.0
    bonding_curve: float = 0.0

    holders:    int   = 0
    top_holder: float = 0.0

    buy_count:  int = 0
    sell_count: int = 0

    buy_volume:  float = 0.0
    sell_volume: float = 0.0

    migrated: bool = False
    called:   bool = False

    created_at: float = field(default_factory=time.time)
    buys: deque = field(default_factory=lambda: deque(maxlen=300))


tokens: dict = {}

# keeps last 20 alerted tokens for /calls
recent_calls: deque = deque(maxlen=20)

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
    cutoff = time.time() - 120
    return sum(1 for x in t.buys if x >= cutoff) / 2


def buy_pressure(t: Token) -> int:
    total = t.buy_count + t.sell_count
    if total == 0:
        return 0
    return int((t.buy_count / total) * 100)


def alpha_score(t: Token) -> float:
    score = 0.0
    score += min(t.sol_in * 0.25, 3)
    score += min(buys_per_min(t) * 0.15, 3)
    score += min(t.holders / 50, 2)
    if t.top_holder < 15:
        score += 1
    if buy_pressure(t) > 70:
        score += 1
    return round(min(score, 10), 1)


def passes_filters(t: Token) -> bool:
    if not (MC_MIN <= t.market_cap <= MC_MAX):
        return False
    if t.sol_in < MIN_SOL_IN:
        return False
    if t.holders < MIN_HOLDERS:
        return False
    if buys_per_min(t) < MIN_BUYS_PER_MIN:
        return False
    if t.top_holder > MAX_TOP_HOLDER:
        return False
    return True

# =========================================================
# ALERT MESSAGE
# =========================================================

def build_alert(t: Token) -> str:
    pressure     = buy_pressure(t)
    total_volume = t.buy_volume + t.sell_volume
    migration    = "Raydium" if t.migrated else "Bonding Curve"
    score        = alpha_score(t)

    return (
        "🚨 EARLY GEM DETECTED 🚨\n\n"
        f"🪙 Token: {t.name} ({t.symbol})\n\n"
        f"💰 Market Cap:    ${fmt(t.market_cap)}\n"
        f"💧 Liquidity:     {t.sol_in:.2f} SOL\n"
        f"📊 Volume:        {total_volume:.2f} SOL\n"
        f"👥 Holders:       {t.holders}\n"
        f"📈 Buy Pressure:  {pressure}%\n"
        f"⚡ Buys/Sells:    {t.buy_count} / {t.sell_count}\n"
        f"🏆 Top Holder:    {t.top_holder:.1f}%\n"
        f"🚀 Status:        {migration}\n"
        f"🔥 Alpha Score:   {score}/10\n\n"
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
            InlineKeyboardButton("⚡ Photon", url=photon),
            InlineKeyboardButton("🐂 BullX",  url=bullx),
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
        # save to recent calls
        recent_calls.append({
            "name":       t.name,
            "symbol":     t.symbol,
            "mint":       t.mint,
            "market_cap": t.market_cap,
            "score":      alpha_score(t),
            "time":       time.time(),
        })
    except Exception as e:
        log.error(f"Failed to send alert: {e}")

# =========================================================
# COMMAND HANDLERS
# =========================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = (
        "👋 GemStalker is live!\n\n"
        "Scanning pump.fun in real-time.\n"
        "You will be alerted here when a gem passes the filters.\n\n"
        "Commands:\n"
        "  /start   - Show this message\n"
        "  /status  - Live tracking stats\n"
        "  /calls   - Last 5 gems alerted\n"
        "  /filters - Show current filters\n\n"
        "Use /filters to see what tokens pass."
    )
    await update.message.reply_text(msg)


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    total    = len(tokens)
    alerted  = sum(1 for t in tokens.values() if t.called)
    watching = total - alerted

    msg = (
        "📡 GemStalker Status\n\n"
        f"  Tokens tracked:  {total}\n"
        f"  Alerts sent:     {alerted}\n"
        f"  Still watching:  {watching}\n"
        f"  SOL Price used:  ${SOL_PRICE}"
    )
    await update.message.reply_text(msg)


async def cmd_calls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_calls:
        await update.message.reply_text(
            "No calls yet. Watching the market...\n"
            "Try /status to see how many tokens are being tracked."
        )
        return

    lines = ["🔥 Recent Calls (last 20):\n"]
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
    msg = (
        "⚙️ Current Filters:\n\n"
        f"  Market Cap:      ${fmt(MC_MIN)} - ${fmt(MC_MAX)}\n"
        f"  Min SOL In:      {MIN_SOL_IN} SOL\n"
        f"  Min Holders:     {MIN_HOLDERS}\n"
        f"  Min Buys/Min:    {MIN_BUYS_PER_MIN}\n"
        f"  Max Top Holder:  {MAX_TOP_HOLDER}%\n\n"
        "Edit these values in the code and redeploy to change them."
    )
    await update.message.reply_text(msg)

# =========================================================
# EVENT HANDLER
# =========================================================

async def handle_event(app: Application, msg: dict) -> None:
    tx_type = msg.get("txType")
    mint    = msg.get("mint")

    if not mint:
        return

    if mint not in tokens:
        tokens[mint] = Token(mint=mint)

    t = tokens[mint]

    t.name   = msg.get("name",   t.name)
    t.symbol = msg.get("symbol", t.symbol)

    market_cap_sol = float(msg.get("marketCapSol", 0) or 0)
    t.market_cap   = market_cap_sol * SOL_PRICE

    sol_amount = float(msg.get("solAmount", 0) or 0)

    if tx_type == "buy":
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

    if not t.called and passes_filters(t):
        t.called = True
        log.info(f"ALERT -> {t.name} ({t.symbol}) | MC=${fmt(t.market_cap)}")
        await send_alert(app, t)

# =========================================================
# WEBSOCKET LOOP
# =========================================================

async def websocket_loop(app: Application) -> None:
    while True:
        try:
            async with websockets.connect(
                PUMP_WS,
                ping_interval=20,
                ping_timeout=20,
            ) as ws:
                log.info("Connected to pump.fun WebSocket")

                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeTokenTrade"}))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        await handle_event(app, msg)
                    except Exception as e:
                        log.error(f"Event error: {e}")

        except Exception as e:
            log.error(f"WebSocket dropped: {e} — reconnecting in 5s")
            await asyncio.sleep(5)

# =========================================================
# STARTUP HOOK
# =========================================================

async def post_init(app: Application) -> None:
    log.info("Starting WebSocket listener...")
    asyncio.create_task(websocket_loop(app))

# =========================================================
# HEALTH SERVER
# =========================================================

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

    def log_message(self, format, *args):
        pass


def run_health_server() -> None:
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log.info(f"Health server on port {port}")
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

    app.post_init = post_init

    log.info("GemStalker started")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
