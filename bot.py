import os
import re
import time
import asyncio
import logging
import datetime
import threading
from collections import deque
import httpx
from flask import Flask
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')

DEX_PROFILES = 'https://api.dexscreener.com/token-profiles/latest/v1'
DEX_BOOSTS = 'https://api.dexscreener.com/token-boosts/latest/v1'
DEX_TOKEN = 'https://api.dexscreener.com/latest/dex/tokens/'
PUMP_FUN = 'https://frontend-api.pump.fun/coins?limit=20&sort=created_timestamp&order=DESC'
SOL_RPC = 'https://api.mainnet-beta.solana.com'

http = httpx.AsyncClient(timeout=10)
flask_app = Flask(__name__)

# ── shared state ────────────────────────────────────────────────────────────
seen_tokens: set = set()          # addresses we've already alerted on
calls: deque = deque(maxlen=100)  # history of alerted tokens (for /calls)
tracked: dict = {}                # address -> {'name', 'price', 'alerted_at', 'mc'}
bot_start_time: float = time.time()


# ── helpers ─────────────────────────────────────────────────────────────────

def fmt_num(n: float) -> str:
    """Format large numbers with K/M/B suffix."""
    if n is None:
        return "?"
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:.4f}"


def pct_change(old: float, new: float) -> str:
    if not old:
        return "N/A"
    change = ((new - old) / old) * 100
    arrow = "🟢" if change >= 0 else "🔴"
    return f"{arrow} {change:+.1f}%"


async def fetch_json(url: str) -> dict | list | None:
    try:
        r = await http.get(url)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"fetch_json failed {url}: {e}")
        return None


async def get_token_info(address: str) -> dict | None:
    data = await fetch_json(DEX_TOKEN + address)
    if not data:
        return None
    pairs = data.get('pairs') or []
    if not pairs:
        return None
    p = pairs[0]
    return {
        'address': address,
        'name': p.get('baseToken', {}).get('name', 'Unknown'),
        'symbol': p.get('baseToken', {}).get('symbol', '?'),
        'price': float(p.get('priceUsd') or 0),
        'mc': float(p.get('fdv') or 0),
        'volume': float(p.get('volume', {}).get('h24') or 0),
        'liquidity': float(p.get('liquidity', {}).get('usd') or 0),
        'chain': p.get('chainId', 'unknown'),
        'dex': p.get('dexId', 'unknown'),
        'url': p.get('url', ''),
        'age_h': (time.time() - (p.get('pairCreatedAt') or time.time() * 1000) / 1000) / 3600,
    }


def build_alert(token: dict, tag: str = "📡 NEW GEM") -> str:
    return (
        f"{tag}\n"
        f"🪙 *{token['name']}* (${token['symbol']})\n"
        f"💵 Price: `${token['price']:.8f}`\n"
        f"💎 MC: `${fmt_num(token['mc'])}`\n"
        f"💧 Liq: `${fmt_num(token['liquidity'])}`\n"
        f"📊 Vol 24h: `${fmt_num(token['volume'])}`\n"
        f"⛓ Chain: `{token['chain']}` | DEX: `{token['dex']}`\n"
        f"🔗 {token['url']}"
    )


# ── flask ────────────────────────────────────────────────────────────────────

@flask_app.route('/')
def health():
    return {'status': 'alive'}


def run_flask():
    flask_app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8080)))


# ── command handlers ─────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start — greet the user."""
    await update.message.reply_text(
        "👾 *GemStalker is live!*\n\n"
        "I hunt fresh tokens across Solana and EVM chains.\n\n"
        "Commands:\n"
        "/scan — manually scan for new gems right now\n"
        "/calls — show recent alerts\n"
        "/status — bot health & stats\n\n"
        "Automatic scans run every 20 seconds.",
        parse_mode='Markdown'
    )


async def scan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /scan — trigger an immediate token scan and report results."""
    await update.message.reply_text("🔍 Scanning now…")
    found = await do_scan()
    if not found:
        await update.message.reply_text("😴 Nothing new at the moment. Try again soon.")
    else:
        for token in found:
            await update.message.reply_text(
                build_alert(token, tag="🔍 MANUAL SCAN"),
                parse_mode='Markdown'
            )


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /status — show uptime and basic stats."""
    uptime_s = int(time.time() - bot_start_time)
    uptime = str(datetime.timedelta(seconds=uptime_s))
    await update.message.reply_text(
        f"✅ *GemStalker Status*\n"
        f"⏱ Uptime: `{uptime}`\n"
        f"👀 Tokens seen: `{len(seen_tokens)}`\n"
        f"📣 Calls made: `{len(calls)}`\n"
        f"🎯 Tracking: `{len(tracked)}` tokens",
        parse_mode='Markdown'
    )


async def calls_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /calls — list recent alerts."""
    if not calls:
        await update.message.reply_text("No calls yet.")
        return
    lines = []
    for i, c in enumerate(reversed(calls), 1):
        lines.append(f"{i}. *{c['name']}* (${c['symbol']}) — MC `${fmt_num(c['mc'])}` at {c['time']}")
    await update.message.reply_text(
        "📣 *Recent Calls*\n\n" + "\n".join(lines),
        parse_mode='Markdown'
    )


async def msg_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle plain text messages — check if it looks like a token address."""
    text = update.message.text.strip()
    # Solana address: base58, ~32-44 chars; EVM address: 0x + 40 hex chars
    sol_pattern = re.compile(r'^[1-9A-HJ-NP-Za-km-z]{32,44}$')
    evm_pattern = re.compile(r'^0x[0-9a-fA-F]{40}$')

    if sol_pattern.match(text) or evm_pattern.match(text):
        await update.message.reply_text(f"🔎 Looking up `{text}`…", parse_mode='Markdown')
        token = await get_token_info(text)
        if token:
            await update.message.reply_text(
                build_alert(token, tag="🔎 TOKEN LOOKUP"),
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text("❌ Couldn't find that token on DexScreener.")
    else:
        await update.message.reply_text(
            "🤖 Send a token address to look it up, or use /scan, /calls, /status."
        )


# ── core scan logic ──────────────────────────────────────────────────────────

async def do_scan() -> list[dict]:
    """Fetch latest tokens from DexScreener profiles + boosts + PumpFun,
    filter out already-seen ones, return new gems."""
    new_gems = []
    addresses = set()

    # 1. DexScreener profiles
    profiles = await fetch_json(DEX_PROFILES)
    if isinstance(profiles, list):
        for p in profiles:
            addr = p.get('tokenAddress') or p.get('address')
            if addr:
                addresses.add(addr)

    # 2. DexScreener boosts
    boosts = await fetch_json(DEX_BOOSTS)
    if isinstance(boosts, list):
        for b in boosts:
            addr = b.get('tokenAddress') or b.get('address')
            if addr:
                addresses.add(addr)

    # 3. PumpFun newest coins
    pump = await fetch_json(PUMP_FUN)
    if isinstance(pump, list):
        for coin in pump:
            addr = coin.get('mint')
            if addr:
                addresses.add(addr)

    # Filter unseen
    fresh = [a for a in addresses if a not in seen_tokens]

    for addr in fresh[:10]:  # cap per cycle to avoid spam
        seen_tokens.add(addr)
        token = await get_token_info(addr)
        if token and token['liquidity'] > 1000:  # basic quality filter
            new_gems.append(token)
            calls.appendleft({
                'name': token['name'],
                'symbol': token['symbol'],
                'mc': token['mc'],
                'address': addr,
                'time': datetime.datetime.utcnow().strftime('%H:%M UTC'),
            })
            # start tracking it
            tracked[addr] = {
                'name': token['name'],
                'symbol': token['symbol'],
                'price': token['price'],
                'mc': token['mc'],
                'alerted_at': time.time(),
            }

    return new_gems


# ── job queue callbacks ──────────────────────────────────────────────────────

async def monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic job: scan for new tokens and push alerts to CHAT_ID."""
    if not CHAT_ID:
        logger.warning("CHAT_ID not set — skipping monitor_job alert")
        return
    try:
        new_gems = await do_scan()
        for token in new_gems:
            await context.bot.send_message(
                chat_id=CHAT_ID,
                text=build_alert(token),
                parse_mode='Markdown'
            )
    except Exception as e:
        logger.error(f"monitor_job error: {e}")


async def track_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Periodic job: check price movement on tracked tokens and alert on big moves."""
    if not CHAT_ID or not tracked:
        return
    try:
        for addr, info in list(tracked.items()):
            token = await get_token_info(addr)
            if not token:
                continue
            old_price = info['price']
            new_price = token['price']
            if old_price and new_price:
                change_pct = ((new_price - old_price) / old_price) * 100
                if abs(change_pct) >= 20:  # alert on ±20% moves
                    direction = "🚀 PUMPING" if change_pct > 0 else "💀 DUMPING"
                    await context.bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"{direction}\n"
                            f"*{info['name']}* (${info['symbol']})\n"
                            f"Price: `${fmt_num(old_price)}` → `${fmt_num(new_price)}`\n"
                            f"Change: {pct_change(old_price, new_price)}\n"
                            f"MC: `${fmt_num(token['mc'])}`"
                        ),
                        parse_mode='Markdown'
                    )
                    # update stored price
                    tracked[addr]['price'] = new_price
                    tracked[addr]['mc'] = token['mc']
    except Exception as e:
        logger.error(f"track_job error: {e}")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    threading.Thread(target=run_flask, daemon=True).start()
    logger.info('Flask started')

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('scan', scan))
    app.add_handler(CommandHandler('status', status))
    app.add_handler(CommandHandler('calls', calls_cmd))   # renamed to avoid shadowing deque
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            msg_handler
        )
    )

    jq = app.job_queue
    jq.run_repeating(monitor_job, interval=20, first=5)
    jq.run_repeating(track_job, interval=120, first=60)

    logger.info('GemStalker running')
    app.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()
