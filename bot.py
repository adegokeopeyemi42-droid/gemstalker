# main.py
import os
import time
import json
import asyncio
import logging
from dataclasses import dataclass, field
from collections import deque

import httpx
import websockets

from telegram import (
InlineKeyboardMarkup,
InlineKeyboardButton,
)

from telegram.ext import (
Application,
)

# =========================================================

# CONFIG

# =========================================================

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PUMP_WS = "wss://pumpportal.fun/api/data"

SOL_PRICE = 150

# =========================================================

# FILTERS

# =========================================================

MC_MIN = 5_000
MC_MAX = 50_000

MIN_SOL_IN = 6
MIN_HOLDERS = 20
MIN_BUYS_PER_MIN = 10

MAX_TOP_HOLDER = 20

# =========================================================

# LOGGING

# =========================================================

logging.basicConfig(
level=logging.INFO,
format="%(asctime)s | %(levelname)s | %(message)s"
)

log = logging.getLogger(**name**)

# =========================================================

# DATA MODEL

# =========================================================

@dataclass
class Token:

```
mint: str

name: str = "Unknown"
symbol: str = "?"

market_cap: float = 0
sol_in: float = 0

holders: int = 0
top_holder: float = 0

bonding_curve: float = 0

buy_count: int = 0
sell_count: int = 0

buy_volume: float = 0
sell_volume: float = 0

migrated: bool = False

called: bool = False

created_at: float = field(default_factory=time.time)

buys: deque = field(default_factory=lambda: deque(maxlen=300))
```

tokens = {}

# =========================================================

# HELPERS

# =========================================================

def fmt(n):

```
n = float(n)

if n >= 1_000_000:
    return f"{n/1_000_000:.2f}M"

if n >= 1_000:
    return f"{n/1_000:.2f}K"

return f"{n:.2f}"
```

def buys_per_min(t):

```
cutoff = time.time() - 120

return sum(1 for x in t.buys if x >= cutoff) / 2
```

def buy_pressure(t):

```
total = t.buy_count + t.sell_count

if total == 0:
    return 0

return int((t.buy_count / total) * 100)
```

def alpha_score(t):

```
score = 0

score += min(t.sol_in * 0.25, 3)

score += min(buys_per_min(t) * 0.15, 3)

score += min(t.holders / 50, 2)

if t.top_holder < 15:
    score += 1

if buy_pressure(t) > 70:
    score += 1

return round(min(score, 10), 1)
```

def passes_filters(t):

```
if t.market_cap < MC_MIN:
    return False

if t.market_cap > MC_MAX:
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
```

# =========================================================

# ALERT UI

# =========================================================

def build_alert(t):

```
pressure = buy_pressure(t)

total_volume = t.buy_volume + t.sell_volume

migration = (
    "🚀 Raydium"
    if t.migrated
    else "⏳ Bonding Curve"
)

score = alpha_score(t)

return f"""
```

🚨 EARLY GEM DETECTED 🚨

🪙 Token:
{t.name} ({t.symbol})

💰 Market Cap:
${fmt(t.market_cap)}

💧 Liquidity:
{t.sol_in:.2f} SOL

📊 Volume:
{total_volume:.2f} SOL

👥 Holders:
{t.holders}

📈 Buy Pressure:
{pressure}%

⚡ Buys/Sells:
{t.buy_count} / {t.sell_count}

🏆 Top Holder:
{t.top_holder:.1f}%

🚀 Status:
{migration}

🔥 Alpha Score:
{score}/10

━━━━━━━━━━━━━━━

📍 Contract:
`{t.mint}`

━━━━━━━━━━━━━━━

🔗 Links:
Photon • BullX • Dex
"""

# =========================================================

# SEND ALERT

# =========================================================

async def send_alert(app, t):

```
photon = f"https://photon-sol.tinyastro.io/en/lp/{t.mint}"

bullx = (
    f"https://bullx.io/terminal"
    f"?chainId=1399811149&address={t.mint}"
)

dex = f"https://dexscreener.com/solana/{t.mint}"

keyboard = InlineKeyboardMarkup([

    [
        InlineKeyboardButton(
            "Photon",
            url=photon
        ),

        InlineKeyboardButton(
            "BullX",
            url=bullx
        ),
    ],

    [
        InlineKeyboardButton(
            "Dex",
            url=dex
        ),
    ]

])

await app.bot.send_message(

    chat_id=CHAT_ID,

    text=build_alert(t),

    parse_mode="Markdown",

    disable_web_page_preview=True,

    reply_markup=keyboard,
)
```

# =========================================================

# EVENT HANDLER

# =========================================================

async def handle_event(app, msg):

```
tx_type = msg.get("txType")

mint = msg.get("mint")

if not mint:
    return

if mint not in tokens:

    tokens[mint] = Token(mint=mint)

t = tokens[mint]

t.name = msg.get("name", t.name)

t.symbol = msg.get("symbol", t.symbol)

market_cap_sol = float(
    msg.get("marketCapSol", 0)
)

t.market_cap = market_cap_sol * SOL_PRICE

sol_amount = (
    float(msg.get("solAmount", 0))
    / 1_000_000_000
)

# ==========================================
# BUY
# ==========================================

if tx_type == "buy":

    t.buy_count += 1

    t.buy_volume += sol_amount

    t.sol_in += sol_amount

    t.buys.append(time.time())

# ==========================================
# SELL
# ==========================================

if tx_type == "sell":

    t.sell_count += 1

    t.sell_volume += sol_amount

# ==========================================
# BONDING
# ==========================================

t.bonding_curve = float(
    msg.get("bondingCurveProgress", 0)
)

# ==========================================
# MIGRATION
# ==========================================

if msg.get("raydiumPool"):
    t.migrated = True

# ==========================================
# HOLDERS
# ==========================================

t.holders = max(
    t.holders,
    int(msg.get("holderCount", 0))
)

# ==========================================
# ALERT
# ==========================================

if not t.called and passes_filters(t):

    t.called = True

    log.info(
        f"ALERT -> {t.name} "
        f"MC=${fmt(t.market_cap)}"
    )

    await send_alert(app, t)
```

# =========================================================

# WEBSOCKET LOOP

# =========================================================

async def websocket_loop(app):

```
while True:

    try:

        async with websockets.connect(
            PUMP_WS,
            ping_interval=20,
            ping_timeout=20,
        ) as ws:

            log.info("Connected to pump.fun")

            await ws.send(json.dumps({

                "method": "subscribeNewToken"

            }))

            await ws.send(json.dumps({

                "method": "subscribeTokenTrade"

            }))

            async for raw in ws:

                try:

                    msg = json.loads(raw)

                    await handle_event(app, msg)

                except Exception as e:

                    log.error(e)

    except Exception as e:

        log.error(f"WS reconnect: {e}")

        await asyncio.sleep(5)
```

# =========================================================

# STARTUP

# =========================================================

async def post_init(app):

```
asyncio.create_task(
    websocket_loop(app)
)
```

# =========================================================

# MAIN

# =========================================================

def main():

```
app = (
    Application
    .builder()
    .token(TG_TOKEN)
    .build()
)

app.post_init = post_init

log.info("GemStalker started")

app.run_polling(
    drop_pending_updates=True
)
```

if **name** == "**main**":

```
main()
```
