import os
import time
import json
import asyncio
import logging
from dataclasses import dataclass, field
from collections import deque

import httpx
import websockets
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO
)

log = logging.getLogger(__name__)

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("CHAT_ID", "")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")

PUMP_WS = "wss://pumpportal.fun/api/data"

SOL_PRICE = 150

F_MC_MIN = 5_000
F_MC_MAX = 50_000
F_HOLDERS_MIN = 20
F_TOP_HOLDER_MAX = 20
F_MIG_PROB_MIN = 60

@dataclass
class TokenState:
    mint: str
    name: str = "Unknown"
    symbol: str = "?"
    market_cap: float = 0
    holders: int = 0
    top_holder: float = 0
    bonding_pct: float = 0
    sol_in: float = 0
    migrated: bool = False
    buy_times: deque = field(default_factory=lambda: deque(maxlen=200))
    buy_count: int = 0
    sell_count: int = 0
    buy_volume_sol: float = 0
    sell_volume_sol: float = 0
    called: bool = False

tokens = {}

http = httpx.AsyncClient(timeout=10)

def fmt(n):
    n = float(n)
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n/1_000:.2f}K"
    return f"{n:.2f}"

def buys_per_minute(s):
    cutoff = time.time() - 120
    return sum(1 for t in s.buy_times if t >= cutoff) / 2

def migration_probability(s):
    score = 0

    score += min(s.bonding_pct, 40)
    score += min(buys_per_minute(s), 25)

    if s.holders >= 100:
        score += 20
    elif s.holders >= 40:
        score += 10

    if s.sol_in >= 10:
        score += 15

    return min(int(score), 100)

def alpha_score(s):
    score = 0

    score += min(s.sol_in * 0.3, 3)
    score += min(buys_per_minute(s) * 0.1, 2)
    score += min(s.holders / 50, 2)
    score += min(migration_probability(s) / 20, 2)

    if s.top_holder < 15:
        score += 1

    return round(min(score, 10), 1)

def passes_filters(s):
    if not (F_MC_MIN <= s.market_cap <= F_MC_MAX):
        return False

    if s.holders < F_HOLDERS_MIN:
        return False

    if s.top_holder > F_TOP_HOLDER_MAX:
        return False

    if migration_probability(s) < F_MIG_PROB_MIN:
        return False

    return True

def build_alert(s):
    pressure = 0

    total = s.buy_count + s.sell_count

    if total > 0:
        pressure = int((s.buy_count / total) * 100)

    volume = s.buy_volume_sol + s.sell_volume_sol

    migration = "Migrated" if s.migrated else "Bonding Curve"

    photon = f"https://photon-sol.tinyastro.io/en/lp/{s.mint}"
    bullx = f"https://bullx.io/terminal?chainId=1399811149&address={s.mint}"
    dex = f"https://dexscreener.com/solana/{s.mint}"

    return f"""
🚨 EARLY GEM DETECTED 🚨

🪙 Token:
{s.name} ({s.symbol})

💰 Market Cap:
${fmt(s.market_cap)}

💧 Liquidity:
{s.sol_in:.2f} SOL

📊 Volume:
{volume:.2f} SOL

👥 Holders:
{s.holders}

📈 Buy Pressure:
{pressure}%

⚡ Buys/Sells:
{s.buy_count} / {s.sell_count}

🏆 Top Holder:
{s.top_holder:.1f}%

🚀 Migration:
{migration}

🔥 Alpha Score:
{alpha_score(s)} / 10

━━━━━━━━━━━━━━━

📍 Contract:
`{s.mint}`

━━━━━━━━━━━━━━━

🔗 Links:
Photon | BullX | Dex
"""

async def send_alert(app, s):
    keyboard = [
        [
            InlineKeyboardButton(
                "Photon",
                url=f"https://photon-sol.tinyastro.io/en/lp/{s.mint}"
            ),
            InlineKeyboardButton(
                "BullX",
                url=f"https://bullx.io/terminal?chainId=1399811149&address={s.mint}"
            )
        ],
        [
            InlineKeyboardButton(
                "Dex",
                url=f"https://dexscreener.com/solana/{s.mint}"
            )
        ]
    ]

    await app.bot.send_message(
        chat_id=CHAT_ID,
        text=build_alert(s),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(keyboard),
        disable_web_page_preview=True
    )

async def handle_event(app, msg):
    tx_type = msg.get("txType", "")
    mint = msg.get("mint")

    if not mint:
        return

    if mint not in tokens:
        tokens[mint] = TokenState(mint=mint)

    s = tokens[mint]

    s.name = msg.get("name", s.name)
    s.symbol = msg.get("symbol", s.symbol)

    mc_sol = float(msg.get("marketCapSol", 0))
    s.market_cap = mc_sol * SOL_PRICE

    sol_amount = float(msg.get("solAmount", 0)) / 1e9

    if tx_type == "buy":
        s.buy_count += 1
        s.buy_volume_sol += sol_amount
        s.sol_in += sol_amount
        s.buy_times.append(time.time())

    if tx_type == "sell":
        s.sell_count += 1
        s.sell_volume_sol += sol_amount

    s.bonding_pct = float(msg.get("bondingCurveProgress", 0))

    if not s.called and passes_filters(s):
        s.called = True
        log.info(f"ALERT: {s.name}")
        await send_alert(app, s)

async def pump_ws_loop(app):
    while True:
        try:
            async with websockets.connect(PUMP_WS) as ws:

                await ws.send(json.dumps({
                    "method": "subscribeNewToken"
                }))

                await ws.send(json.dumps({
                    "method": "subscribeTokenTrade"
                }))

                log.info("Connected to pump.fun WS")

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        await handle_event(app, msg)
                    except Exception as e:
                        log.error(e)

        except Exception as e:
            log.error(f"WS reconnect: {e}")
            await asyncio.sleep(5)

async def post_init(app):
    asyncio.create_task(pump_ws_loop(app))

def main():
    app = Application.builder().token(TG_TOKEN).build()

    app.post_init = post_init

    log.info("GemStalker Started")

    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
