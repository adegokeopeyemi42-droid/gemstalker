import os
import re
import time
import asyncio
import logging
import datetime
import threading
from collections import deque
from typing import Optional

import httpx
from flask import Flask
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ---------------------------------------------------
# Logging
# ---------------------------------------------------

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------
# Environment
# ---------------------------------------------------

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TOKEN:
    raise ValueError("Missing TELEGRAM_BOT_TOKEN")

# ---------------------------------------------------
# URLs
# ---------------------------------------------------

DEX_PROFILES = "https://api.dexscreener.com/token-profiles/latest/v1"
DEX_BOOSTS = "https://api.dexscreener.com/token-boosts/latest/v1"
DEX_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/"
PUMP_FUN = "https://frontend-api.pump.fun/coins?limit=20&sort=created_timestamp&order=DESC"
SOL_RPC = "https://api.mainnet-beta.solana.com"

# ---------------------------------------------------
# Filters
# ---------------------------------------------------

MIN_MC = 25_000
MAX_MC = 200_000
MIN_LIQ = 15_000
MIN_VOL = 30_000
MIN_SCORE = 70

# ---------------------------------------------------
# Globals
# ---------------------------------------------------

seen_tokens = set()
tracked_tokens = {}
daily_calls = deque(maxlen=500)

stats = {
    "cycles": 0,
    "checked": 0,
    "alerts": 0,
}

# ---------------------------------------------------
# HTTP Client
# ---------------------------------------------------

client = httpx.AsyncClient(
    timeout=10,
    limits=httpx.Limits(max_connections=20)
)

# ---------------------------------------------------
# Flask
# ---------------------------------------------------

flask_app = Flask(__name__)

@flask_app.route("/")
def health():
    return {
        "status": "alive",
        "cycles": stats["cycles"],
        "alerts": stats["alerts"],
    }

def run_flask():
    port = int(os.getenv("PORT", 8080))
    flask_app.run(host="0.0.0.0", port=port)

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

def is_solana_address(text: str) -> bool:
    return bool(
        re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", text)
    )

def fmt_usd(v) -> str:
    try:
        v = float(v)

        if v >= 1_000_000:
            return f"${v/1_000_000:.2f}M"

        if v >= 1_000:
            return f"${v/1_000:.1f}K"

        return f"${v:,.0f}"

    except:
        return "N/A"

# ---------------------------------------------------
# DexScreener
# ---------------------------------------------------

async def get_pair(address: str):

    try:
        r = await client.get(f"{DEX_TOKEN}{address}")
        data = r.json()

        pairs = data.get("pairs", [])

        sol_pairs = [
            p for p in pairs
            if p.get("chainId") == "solana"
        ]

        if not sol_pairs:
            return None

        return max(
            sol_pairs,
            key=lambda p: float(
                p.get("liquidity", {}).get("usd", 0) or 0
            )
        )

    except Exception as e:
        logger.debug(f"Pair fetch error: {e}")
        return None

# ---------------------------------------------------
# Solana Checks
# ---------------------------------------------------

async def rpc(method: str, params: list):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    }

    try:
        r = await client.post(SOL_RPC, json=payload)
        return r.json().get("result")

    except Exception as e:
        logger.debug(f"RPC error: {e}")
        return None

async def check_chain(address: str):

    result = {
        "mint_revoked": True,
        "freeze_revoked": True,
        "top10_pct": None,
        "dev_pct": None,
        "whale_alert": False,
    }

    try:

        account, supply, largest = await asyncio.gather(
            rpc("getAccountInfo", [address, {"encoding": "jsonParsed"}]),
            rpc("getTokenSupply", [address]),
            rpc("getTokenLargestAccounts", [address]),
        )

        info = account["value"]["data"]["parsed"]["info"]

        result["mint_revoked"] = (
            info.get("mintAuthority") is None
        )

        result["freeze_revoked"] = (
            info.get("freezeAuthority") is None
        )

        total_supply = float(
            supply["value"]["uiAmount"] or 0
        )

        wallets = largest["value"]

        if total_supply > 0 and wallets:

            amounts = [
                float(x.get("uiAmount") or 0)
                for x in wallets
            ]

            result["top10_pct"] = (
                sum(amounts[:10]) / total_supply * 100
            )

            result["dev_pct"] = (
                amounts[0] / total_supply * 100
            )

            result["whale_alert"] = any(
                a / total_supply * 100 > 15
                for a in amounts
            )

    except Exception as e:
        logger.debug(f"Chain check error: {e}")

    return result

# ---------------------------------------------------
# Score
# ---------------------------------------------------

def score_token(
    volume,
    liquidity,
    buy_pct,
    mint_revoked,
    freeze_revoked,
):

    score = 0

    if volume > 100_000:
        score += 25

    elif volume > 50_000:
        score += 15

    if liquidity > 50_000:
        score += 25

    elif liquidity > 20_000:
        score += 15

    if 60 <= buy_pct <= 75:
        score += 25

    if mint_revoked:
        score += 15

    if freeze_revoked:
        score += 10

    return min(score, 100)

# ---------------------------------------------------
# Build Message
# ---------------------------------------------------

def build_card(
    name,
    symbol,
    mc,
    liquidity,
    volume,
    score,
    address,
):

    return f"""
🚨 <b>HIGH POTENTIAL GEM</b>

<b>{name}</b> (${symbol})

MC: {fmt_usd(mc)}
Liquidity: {fmt_usd(liquidity)}
Volume: {fmt_usd(volume)}

Score: <b>{score}/100</b>

<a href="https://dexscreener.com/solana/{address}">
DexScreener
</a>
"""

# ---------------------------------------------------
# Process Token
# ---------------------------------------------------

async def process_token(
    address,
    source,
    name,
    symbol,
    bot,
):

    if address in seen_tokens:
        return

    stats["checked"] += 1

    pair = await get_pair(address)

    if not pair:
        return

    mc = float(pair.get("marketCap", 0) or 0)

    liquidity = float(
        pair.get("liquidity", {}).get("usd", 0) or 0
    )

    volume = float(
        pair.get("volume", {}).get("h24", 0) or 0
    )

    txns = pair.get("txns", {}).get("h24", {})

    buys = txns.get("buys", 0)
    sells = txns.get("sells", 0)

    total = buys + sells

    buy_pct = (
        buys / total * 100
        if total > 0 else 0
    )

    if mc < MIN_MC or mc > MAX_MC:
        return

    if liquidity < MIN_LIQ:
        return

    if volume < MIN_VOL:
        return

    chain = await check_chain(address)

    score = score_token(
        volume=volume,
        liquidity=liquidity,
        buy_pct=buy_pct,
        mint_revoked=chain["mint_revoked"],
        freeze_revoked=chain["freeze_revoked"],
    )

    if score < MIN_SCORE:
        return

    seen_tokens.add(address)

    stats["alerts"] += 1

    card = build_card(
        name=name,
        symbol=symbol,
        mc=mc,
        liquidity=liquidity,
        volume=volume,
        score=score,
        address=address,
    )

    try:

        await bot.send_message(
            chat_id=CHAT_ID,
            text=card,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

        logger.info(f"ALERT: {symbol}")

    except Exception as e:
        logger.error(f"Telegram error: {e}")

# ---------------------------------------------------
# Monitor Job
# ---------------------------------------------------

async def monitor_job(
    context: ContextTypes.DEFAULT_TYPE
):

    stats["cycles"] += 1

    bot = context.bot

    try:

        r = await client.get(DEX_PROFILES)

        profiles = r.json()

        solana = [
            p for p in profiles
            if p.get("chainId") == "solana"
        ]

        for p in solana[:20]:

            address = p.get("tokenAddress")

            if not address:
                continue

            await process_token(
                address=address,
                source="Dex",
                name="Unknown",
                symbol="???",
                bot=bot,
            )

    except Exception as e:
        logger.error(f"Monitor error: {e}")

# ---------------------------------------------------
# Commands
# ---------------------------------------------------

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    await update.message.reply_text(
        "GemStalker online."
    )

async def scan(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not context.args:
        await update.message.reply_text(
            "Usage: /scan <address>"
        )
        return

    address = context.args[0]

    pair = await get_pair(address)

    if not pair:
        await update.message.reply_text(
            "No pair found."
        )
        return

    await update.message.reply_text(
        f"Found: {pair['baseToken']['symbol']}"
    )

# ---------------------------------------------------
# Main
# ---------------------------------------------------

def main():

    flask_thread = threading.Thread(
        target=run_flask,
        daemon=True
    )

    flask_thread.start()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("scan", scan))

    jq = app.job_queue

    jq.run_repeating(
        monitor_job,
        interval=20,
        first=5,
    )

    logger.info("GemStalker running")

    app.run_polling()

if __name__ == "__main__":
    main()
