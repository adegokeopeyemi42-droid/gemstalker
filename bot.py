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

SOL_PRICE = 95.0  # updated live every 60s from CoinGecko

# Solana CA: base58, 32-44 chars
SOLANA_CA_RE = re.compile(r"\b[1-9A-HJ-NP-Za-km-z]{32,44}\b")

# =========================================================
# FILTERS  (relaxed — tighten once alerts confirmed firing)
# =========================================================

MC_MIN           = 3_000    # USD
MC_MAX           = 500_000  # USD
MIN_SOL_IN       = 0.3      # SOL cumulative buy inflow
MIN_BUYS         = 2        # total buys seen
MIN_BUYS_PER_MIN = 0.5      # real 60s window

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
    sol_in:        float = 0.0   # cumulative buy inflow (SOL)
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

    buys: deque = field(default_factory=lambda: deque(maxlen=500))


tokens: dict        = {}
recent_calls: deque = deque(maxlen=20)

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

def fmt(n, decimals: int = 2) -> str:
    n = float(n)
    if n >= 1_000_000:
        return f"{n / 1_000_000:.{decimals}f}M"
    if n >= 1_000:
        return f"{n / 1_000:.{decimals}f}K"
    return f"{n:.{decimals}f}"


def buys_per_min(t: Token) -> float:
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
    return f"{secs // 3600}h {(secs % 3600) // 60}m"


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
    while True:
        await asyncio.sleep(300)
        cutoff = time.time() - 2700  # 45 minutes
        stale  = [m for m, t in tokens.items() if t.last_active < cutoff]
        for m in stale:
            del tokens[m]
        if stale:
            log.info(f"[CLEANUP] Removed {len(stale)} stale tokens. Active: {len(tokens)}")

# =========================================================
# STATS LOGGER
# =========================================================

async def log_stats() -> None:
    global event_counter, buy_counter
    while True:
        await asyncio.sleep(60)
        log.info(
            f"[STATS] Events/min: {event_counter} | Buys: {buy_counter} | "
            f"Tokens: {len(tokens)} | Alerts: {sum(1 for t in tokens.values() if t.called)}"
        )
        event_counter = 0
        buy_counter   = 0

# =========================================================
# PAIR SELECTION  (volume-first, not liquidity-first)
# =========================================================

def score_pair(pair: dict) -> float:
    """
    Score a DexScreener pair for freshness/relevance.
    Priority: active txns > volume > liquidity.
    Do NOT rely on liquidity alone — it's often null on fresh launches.
    """
    txns   = pair.get("txns") or {}
    vol    = pair.get("volume") or {}
    liq    = pair.get("liquidity") or {}

    buys1  = int((txns.get("h1") or {}).get("buys",  0))
    sells1 = int((txns.get("h1") or {}).get("sells", 0))
    vol1   = float(vol.get("h1",  0) or 0)
    vol24  = float(vol.get("h24", 0) or 0)
    liq_usd = float(liq.get("usd", 0) or 0)

    return (buys1 + sells1) * 10 + vol1 * 0.01 + vol24 * 0.001 + liq_usd * 0.001


def best_sol_pair(pairs: list) -> dict | None:
    sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
    if not sol_pairs:
        return None
    return max(sol_pairs, key=score_pair)

# =========================================================
# CA ANALYZER — API LAYER
# =========================================================

async def fetch_dexscreener(ca: str) -> dict | None:
    url = f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r    = await client.get(url)
            data = r.json()
            pairs = data.get("pairs") or []
            if pairs:
                pair = best_sol_pair(pairs)
                if pair:
                    log.info(f"[DEX] Found pair for {ca[:10]}.. via DexScreener")
                    return pair
    except Exception as e:
        log.warning(f"[DEX] DexScreener failed for {ca[:10]}: {e}")
    return None


async def fetch_geckoterminal(ca: str) -> dict | None:
    """Fallback: GeckoTerminal — free, no key needed."""
    url = f"https://api.geckoterminal.com/api/v2/networks/solana/tokens/{ca}/pools?page=1"
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r    = await client.get(url, headers={"Accept": "application/json"})
            data = r.json()
            pools = (data.get("data") or [])
            if pools:
                # pick pool with highest 24h volume
                best = max(
                    pools,
                    key=lambda p: float(
                        (p.get("attributes") or {}).get("volume_usd", {}).get("h24", 0) or 0
                    )
                )
                attrs = best.get("attributes") or {}
                log.info(f"[GECKO] Found pool for {ca[:10]}.. via GeckoTerminal")
                return {"_source": "gecko", "_attrs": attrs, "_ca": ca}
    except Exception as e:
        log.warning(f"[GECKO] GeckoTerminal failed for {ca[:10]}: {e}")
    return None


def safe_liq(pair: dict):
    """
    Return liquidity USD or None.
    NEVER return 0 when liquidity is simply unavailable.
    """
    liq = pair.get("liquidity")
    if not liq or not isinstance(liq, dict):
        return None
    val = liq.get("usd")
    if val is None:
        return None
    try:
        f = float(val)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None

# =========================================================
# CA ANALYZER — ANALYSIS LOGIC
# =========================================================

def analyze_dex_pair(ca: str, pair: dict) -> str:
    base   = pair.get("baseToken") or {}
    name   = base.get("name",   "Unknown")
    symbol = base.get("symbol", "?")

    mc     = float(pair.get("marketCap") or pair.get("fdv") or 0)
    liq    = safe_liq(pair)                          # None = pending
    txns   = pair.get("txns")   or {}
    vol    = pair.get("volume") or {}

    buys1   = int((txns.get("h1")  or {}).get("buys",   0))
    sells1  = int((txns.get("h1")  or {}).get("sells",  0))
    buys24  = int((txns.get("h24") or {}).get("buys",   0))
    sells24 = int((txns.get("h24") or {}).get("sells",  0))
    vol1    = float(vol.get("h1",  0) or 0)
    vol24   = float(vol.get("h24", 0) or 0)
    pc1h    = float((pair.get("priceChange") or {}).get("h1",  0) or 0)
    pc24h   = float((pair.get("priceChange") or {}).get("h24", 0) or 0)

    total1 = buys1 + sells1
    pressure = int((buys1 / total1) * 100) if total1 else 0

    liq_str = f"${fmt(liq)}" if liq is not None else "Pending"

    # ---- safety flags (ONLY flag liq if confirmed low, not None) ----
    flags = []
    if liq is not None and liq < 5_000:
        flags.append("Low liquidity")
    if vol24 < 500:
        flags.append("Near-zero volume")
    if total1 > 0 and sells1 > buys1 * 2:
        flags.append("Heavy sell pressure")
    if buys1 == 0 and sells1 == 0:
        flags.append("No activity last hour")
    if liq is not None and mc > 0 and (liq / mc) < 0.02:
        flags.append("Thin liq vs MC")

    # ---- AI reads (3-4 bullets max) ----
    reads = []
    if pressure > 68:
        reads.append("Buyers aggressive")
    elif pressure < 35:
        reads.append("Sellers dominating")
    if pc1h > 15:
        reads.append("Strong momentum last hour")
    elif pc1h < -15:
        reads.append("Dumping last hour")
    if vol1 > vol24 * 0.3:
        reads.append("Volume accelerating")
    if mc < 50_000:
        reads.append("Very early stage")
    elif mc < 300_000:
        reads.append("Still early")
    if liq is None:
        reads.append("Liquidity data pending")
    if not reads:
        reads.append("No strong signal")
    reads = reads[:4]

    # ---- status rating ----
    bad = len(flags)
    if bad == 0 and pressure > 60 and vol1 > 3_000:
        status = "🟢 RUNNER"
    elif bad >= 3 or (total1 > 5 and sells1 > buys1 * 2):
        status = "🔴 AVOID"
    elif bad >= 1 or pressure < 45:
        status = "🟠 SPECULATIVE"
    else:
        status = "🟡 WATCHLIST"

    reads_fmt = "\n".join(f"• {r}" for r in reads)
    flags_fmt = "\n".join(f"⚠️ {f}" for f in flags) if flags else "• No major red flags"

    dex    = f"https://dexscreener.com/solana/{ca}"
    photon = f"https://photon-sol.tinyastro.io/en/lp/{ca}"
    bullx  = f"https://bullx.io/terminal?chainId=1399811149&address={ca}"

    return (
        f"━━━━━━━━━━━━━━\n"
        f"🪙 {name} ({symbol})\n\n"
        f"MC      • {('$' + fmt(mc)) if mc else 'N/A'}\n"
        f"LIQ     • {liq_str}\n"
        f"VOL 1H  • ${fmt(vol1)}\n"
        f"VOL 24H • ${fmt(vol24)}\n"
        f"PRESSURE• {pressure}%\n\n"
        f"BUY/SELL• {buys1} / {sells1}  (1h)\n"
        f"CHANGE  • {pc1h:+.1f}%  (1h)  {pc24h:+.1f}%  (24h)\n\n"
        f"SAFETY\n{flags_fmt}\n\n"
        f"AI READ\n{reads_fmt}\n\n"
        f"STATUS\n{status}\n"
        f"━━━━━━━━━━━━━━\n"
        f"[Dex]({dex})  •  [Photon]({photon})  •  [BullX]({bullx})"
    )


def analyze_gecko_data(ca: str, gecko: dict) -> str:
    attrs  = gecko.get("_attrs") or {}
    name   = attrs.get("name", "Unknown")
    symbol = (attrs.get("base_token_price_usd") and "") or "?"

    mc     = float(attrs.get("market_cap_usd") or 0)
    liq    = float(attrs.get("reserve_in_usd") or 0) or None
    vol24  = float((attrs.get("volume_usd") or {}).get("h24", 0) or 0)

    liq_str = f"${fmt(liq)}" if liq else "Pending"

    dex    = f"https://dexscreener.com/solana/{ca}"
    photon = f"https://photon-sol.tinyastro.io/en/lp/{ca}"
    bullx  = f"https://bullx.io/terminal?chainId=1399811149&address={ca}"

    return (
        f"━━━━━━━━━━━━━━\n"
        f"🪙 {name} (via GeckoTerminal)\n\n"
        f"MC      • {('$' + fmt(mc)) if mc else 'N/A'}\n"
        f"LIQ     • {liq_str}\n"
        f"VOL 24H • ${fmt(vol24)}\n\n"
        f"AI READ\n• Limited data — token may be very new\n\n"
        f"STATUS\n🟡 WATCHLIST\n"
        f"━━━━━━━━━━━━━━\n"
        f"[Dex]({dex})  •  [Photon]({photon})  •  [BullX]({bullx})"
    )


async def analyze_ca(ca: str) -> str:
    log.info(f"[CA] Analyzing: {ca}")

    # Try DexScreener first
    pair = await fetch_dexscreener(ca)
    if pair:
        return analyze_dex_pair(ca, pair)

    # Fallback: GeckoTerminal
    gecko = await fetch_geckoterminal(ca)
    if gecko:
        return analyze_gecko_data(ca, gecko)

    # Nothing found
    dex = f"https://dexscreener.com/solana/{ca}"
    return (
        f"━━━━━━━━━━━━━━\n"
        f"❓ No data found\n\n"
        f"{ca}\n\n"
        f"Token may be too new or not yet indexed.\n"
        f"Check manually: {dex}\n"
        f"━━━━━━━━━━━━━━"
    )

# =========================================================
# GEM ALERT MESSAGE  (clean sniper-terminal UI)
# =========================================================

def build_alert(t: Token) -> str:
    pressure  = buy_pressure(t)
    net_flow  = t.buy_volume - t.sell_volume
    score     = alpha_score(t)
    bpm       = buys_per_min(t)
    holders   = str(t.holders) if t.holders else "N/A"
    top       = f"{t.top_holder:.1f}%" if t.top_holder else "N/A"
    status    = "Raydium" if t.migrated else "Bonding Curve"

    # status label
    if score >= 7:
        label = "🟢 RUNNER"
    elif score >= 5:
        label = "🟡 WATCHLIST"
    elif score >= 3:
        label = "🟠 SPECULATIVE"
    else:
        label = "🔴 AVOID"

    # AI reads from live data
    reads = []
    if pressure > 68:
        reads.append("Buyers aggressive")
    elif pressure < 35:
        reads.append("Sell pressure present")
    if bpm > 5:
        reads.append("High buy frequency")
    if net_flow > 0:
        reads.append("Net positive buy flow")
    elif net_flow < -0.5:
        reads.append("Net outflow — caution")
    if t.sol_in > 5:
        reads.append("Strong accumulation")
    if not reads:
        reads.append("Momentum building")
    reads = reads[:4]

    reads_fmt = "\n".join(f"• {r}" for r in reads)

    return (
        f"━━━━━━━━━━━━━━\n"
        f"🚨 GEM DETECTED\n\n"
        f"🪙 {t.name} ({t.symbol})\n"
        f"⏱ Age: {token_age_str(t)}\n\n"
        f"MC       • ${fmt(t.market_cap)}\n"
        f"INFLOW   • {t.sol_in:.2f} SOL\n"
        f"NET FLOW • {net_flow:+.2f} SOL\n"
        f"VOLUME   • {t.buy_volume + t.sell_volume:.2f} SOL\n"
        f"PRESSURE • {pressure}%\n\n"
        f"BUY/SELL • {t.buy_count} / {t.sell_count}\n"
        f"B/MIN    • {bpm:.1f}\n"
        f"HOLDERS  • {holders}\n"
        f"TOP HOLD • {top}\n"
        f"STATUS   • {status}\n"
        f"SCORE    • {score}/10\n\n"
        f"AI READ\n{reads_fmt}\n\n"
        f"SIGNAL\n{label}\n"
        f"━━━━━━━━━━━━━━\n"
        f"`{t.mint}`"
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
            InlineKeyboardButton("📊 Dex",         url=dex),
        ],
    ])

    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=build_alert(t),
            parse_mode="Markdown",
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
        log.info(f"[ALERT SENT] {t.name} ({t.symbol}) MC=${fmt(t.market_cap)}")
    except Exception as e:
        log.error(f"[ALERT ERROR] {e}")

# =========================================================
# COMMANDS
# =========================================================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "👋 GemStalker is live!\n\n"
        "Scanning pump.fun in real-time.\n"
        "Paste any Solana CA for instant analysis.\n\n"
        "Commands:\n"
        "  /start    — This message\n"
        "  /status   — Live tracking stats\n"
        "  /calls    — Last 20 alerts\n"
        "  /filters  — Current filter settings\n"
        "  /analyze  — Analyze a CA"
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
        f"Tokens tracked:  {total}\n"
        f"Alerts sent:     {alerted}\n"
        f"Still watching:  {watching}\n"
        f"SOL Price:       ${SOL_PRICE:.2f}\n"
    )

    if candidates:
        msg += "\nTop candidates:\n"
        for t in candidates:
            _, reason = passes_filters(t)
            msg += (
                f"\n{t.name} ({t.symbol})\n"
                f"  MC:${fmt(t.market_cap)} | "
                f"Inflow:{t.sol_in:.2f} SOL | "
                f"Buys:{t.buy_count}\n"
                f"  Blocking: {reason}\n"
            )

    await update.message.reply_text(msg)


async def cmd_calls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_calls:
        await update.message.reply_text(
            "No calls yet.\nUse /status to see top candidates."
        )
        return

    lines = ["🔥 Recent Calls:\n"]
    for i, c in enumerate(reversed(recent_calls), 1):
        age_min = int((time.time() - c["time"]) / 60)
        age_str = f"{age_min}m ago" if age_min < 60 else f"{age_min // 60}h ago"
        lines.append(
            f"{i}. {c['name']} ({c['symbol']})\n"
            f"   MC: ${fmt(c['market_cap'])} | "
            f"Score: {c['score']}/10 | {age_str}\n"
            f"   {c['mint'][:20]}...\n"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "⚙️ Active Filters:\n\n"
        f"Market Cap:      ${fmt(MC_MIN)} - ${fmt(MC_MAX)}\n"
        f"Min Buy Inflow:  {MIN_SOL_IN} SOL\n"
        f"Min Buys:        {MIN_BUYS}\n"
        f"Min Buys/Min:    {MIN_BUYS_PER_MIN}\n\n"
        f"SOL Price:       ${SOL_PRICE:.2f} (live)\n\n"
        "Note: Holders/top holder not filtered\n"
        "(rarely available in live pump.fun events)"
    )


async def cmd_analyze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /analyze <contract_address>"
        )
        return

    ca = context.args[0].strip()
    if not SOLANA_CA_RE.search(ca):
        await update.message.reply_text("That doesn't look like a valid Solana CA.")
        return

    msg = await update.message.reply_text("🔍 Analyzing...")
    result = await analyze_ca(ca)
    try:
        await msg.edit_text(result, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception:
        await msg.edit_text(result, disable_web_page_preview=True)


async def handle_ca_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or "").strip()

    # full message is a CA
    if SOLANA_CA_RE.fullmatch(text):
        ca = text
    else:
        # CA embedded in a message
        matches = SOLANA_CA_RE.findall(text)
        if not matches:
            return
        ca = matches[0]

    msg = await update.message.reply_text(f"🔍 Analyzing {ca[:12]}...")
    result = await analyze_ca(ca)
    try:
        await msg.edit_text(result, parse_mode="Markdown", disable_web_page_preview=True)
    except Exception:
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

    if tx_type in ("buy", "sell"):
        log.debug(
            f"[{tx_type.upper()}] {mint[:8]}.. "
            f"mcSol={msg.get('marketCapSol', '?')} "
            f"sol={msg.get('solAmount', '?')}"
        )

    if mint not in tokens:
        tokens[mint] = Token(mint=mint)

    t             = tokens[mint]
    t.last_active = time.time()
    t.name        = msg.get("name",   t.name)
    t.symbol      = msg.get("symbol", t.symbol)

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

                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("[WS] Subscribed: subscribeNewToken")

                await ws.send(json.dumps({"method": "subscribeTokenTrade"}))
                log.info("[WS] Subscribed: subscribeTokenTrade")

                heartbeat = time.time()
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        await handle_event(app, msg)

                        if time.time() - heartbeat > 30:
                            heartbeat = time.time()
                            log.info(
                                f"[WS HEARTBEAT] alive | "
                                f"events={event_counter} tokens={len(tokens)}"
                            )
                    except json.JSONDecodeError as e:
                        log.warning(f"[WS] Bad JSON: {e}")
                    except Exception as e:
                        log.error(f"[WS] Event error: {e}")

        except Exception as e:
            reconnect_count += 1
            wait = min(5 * reconnect_count, 30)
            log.error(f"[WS] Disconnected: {e} — retry in {wait}s")
            await asyncio.sleep(wait)

# =========================================================
# STARTUP HOOK
# =========================================================

async def post_init(app: Application) -> None:
    log.info("[INIT] Launching background tasks...")
    asyncio.create_task(update_sol_price())
    asyncio.create_task(websocket_loop(app))
    asyncio.create_task(cleanup_tokens())
    asyncio.create_task(log_stats())
    log.info("[INIT] All tasks running")

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
    log.info(f"[HEALTH] Listening on port {port}")
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
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_ca_message))

    app.post_init = post_init

    log.info("[MAIN] GemStalker starting")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
