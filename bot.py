import os
import logging
import asyncio
import time
import re
import datetime
import threading
import httpx
from flask import Flask
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

logging.basicConfig(
format=”%(asctime)s - %(name)s - %(levelname)s - %(message)s”,
level=logging.INFO,
)
logger = logging.getLogger(**name**)

TOKEN = os.environ[“TELEGRAM_BOT_TOKEN”]
CHAT_ID = os.environ[“CHAT_ID”]

DEXSCREENER_PROFILES_URL = “https://api.dexscreener.com/token-profiles/latest/v1”
DEXSCREENER_BOOSTS_URL = “https://api.dexscreener.com/token-boosts/latest/v1”
DEXSCREENER_TOKENS_URL = “https://api.dexscreener.com/latest/dex/tokens/”
PUMP_FUN_URL = “https://frontend-api.pump.fun/coins?limit=20&sort=created_timestamp&order=DESC”
SOLANA_RPC_URL = “https://api.mainnet-beta.solana.com”

# ── Filter thresholds ──────────────────────────────────────────────────────────

MIN_MC = 25_000
MAX_MC = 200_000
MIN_LIQUIDITY = 15_000
MIN_VOLUME = 30_000
MIN_BUY_PRESSURE = 60.0
MAX_BUY_PRESSURE = 72.0
MIN_SCORE = 70
MIN_AGE_MIN = 4
MAX_AGE_MIN = 30
MAX_TOP10_PCT = 18.0
MAX_DEV_PCT = 3.0

# ── State ──────────────────────────────────────────────────────────────────────

cycle_count = 0
start_time: float = 0.0
seen_tokens: set = set()

stats: dict = {
“cycles”: 0,
“tokens_checked”: 0,
“alerts_sent”: 0,
“no_pair”: 0,
“filter_mc”: 0,
“filter_liquidity”: 0,
“filter_volume”: 0,
“filter_buy_pressure”: 0,
“filter_age”: 0,
“filter_concentration”: 0,
“filter_mint”: 0,
“filter_freeze”: 0,
“filter_score”: 0,
}

tracked_tokens: dict = {}
daily_calls: list = []
daily_reset_date: str = “”

# ── Flask keep-alive server ────────────────────────────────────────────────────

flask_app = Flask(**name**)

@flask_app.route(”/”)
def health():
return {“status”: “GemStalker is alive 💎”, “cycles”: stats[“cycles”], “alerts”: stats[“alerts_sent”]}

def run_flask():
port = int(os.environ.get(“PORT”, 8080))
flask_app.run(host=“0.0.0.0”, port=port)

# ── Helpers ────────────────────────────────────────────────────────────────────

def fmt_usd(val) -> str:
if val in (None, “N/A”):
return “N/A”
try:
v = float(val)
if v >= 1_000_000:
return f”${v / 1_000_000:.2f}M”
if v >= 1_000:
return f”${v / 1_000:.1f}K”
return f”${v:,.0f}”
except (ValueError, TypeError):
return “N/A”

def is_solana_address(text: str) -> bool:
return bool(re.fullmatch(r”[1-9A-HJ-NP-Za-km-z]{32,44}”, text.strip()))

def potential_label(score: int, mc: float) -> str:
if mc < 40_000 and score >= 90:
return “10x 🔥”
if mc < 100_000 and score >= 85:
return “5x 🚀”
if score >= 70:
return “2x ✅”
return “Low ❌”

def build_token_card(
name, symbol, mc, liquidity, volume, age_min, buy_pct,
mint_revoked, freeze_revoked, top10_pct, dev_pct,
holder_count, whale_alert, txns_5m, score, address, chain, source=””
) -> str:
potential = potential_label(score, mc)
dex_link = f”https://dexscreener.com/{chain}/{address}”
src_line = f”📡 {source}\n” if source else “”
top10_str = f”{top10_pct:.1f}%” if top10_pct is not None else “N/A”
dev_str = f”{dev_pct:.1f}%” if dev_pct is not None else “N/A”
holders_str = str(holder_count) if holder_count is not None else “N/A”

```
return (
    f"🚨 *HIGH POTENTIAL GEM*\n"
    f"💎 *{name}* (${symbol})\n"
    f"{src_line}"
    f"━━━━━━━━━━━━━━━━━━━━\n"
    f"📊 MC: {fmt_usd(mc)} | Liq: {fmt_usd(liquidity)}\n"
    f"📈 Vol: {fmt_usd(volume)} | Age: {age_min:.0f}m\n"
    f"🟢 Buys: {buy_pct:.0f}% | 5m txns: {txns_5m}\n"
    f"🔒 Mint: {'✅' if mint_revoked else '❌'} | Freeze: {'✅' if freeze_revoked else '❌'}\n"
    f"📊 Top10: {top10_str} | 👨‍💻 Dev: {dev_str}\n"
    f"🐋 Whale: {'⚠️ YES' if whale_alert else '✅ No'} | 👥 Holders: {holders_str}\n"
    f"━━━━━━━━━━━━━━━━━━━━\n"
    f"🎯 Score: {score}/100\n"
    f"🚀 Potential: {potential}\n"
    f"🔗 [Dexscreener]({dex_link})"
)
```

# ── API calls ──────────────────────────────────────────────────────────────────

async def get_dexscreener_pair(address: str):
try:
async with httpx.AsyncClient(timeout=10) as client:
response = await client.get(f”{DEXSCREENER_TOKENS_URL}{address}”)
response.raise_for_status()
data = response.json()
pairs = data.get(“pairs”) or []
solana_pairs = [p for p in pairs if p.get(“chainId”) == “solana”]
if not solana_pairs:
return None
return max(solana_pairs, key=lambda p: float(p.get(“liquidity”, {}).get(“usd”, 0) or 0))
except Exception as e:
logger.debug(f”Dexscreener pair lookup failed for {address}: {e}”)
return None

async def _rpc(client, method: str, params: list):
try:
resp = await client.post(
SOLANA_RPC_URL,
json={“jsonrpc”: “2.0”, “id”: 1, “method”: method, “params”: params},
timeout=12,
)
resp.raise_for_status()
return resp.json().get(“result”)
except Exception as e:
logger.debug(f”RPC {method} failed: {e}”)
return None

async def check_solana_on_chain(address: str) -> dict:
async with httpx.AsyncClient(timeout=12) as client:
account_info, supply_result, largest_accounts = await asyncio.gather(
_rpc(client, “getAccountInfo”, [address, {“encoding”: “jsonParsed”}]),
_rpc(client, “getTokenSupply”, [address]),
_rpc(client, “getTokenLargestAccounts”, [address]),
)

```
result = {
    "mint_revoked": True,
    "freeze_revoked": True,
    "top10_pct": None,
    "dev_pct": None,
    "dev_address": None,
    "whale_alert": False,
    "holder_count": None,
}

try:
    info = account_info["value"]["data"]["parsed"]["info"]
    result["mint_revoked"] = info.get("mintAuthority") is None
    result["freeze_revoked"] = info.get("freezeAuthority") is None
except Exception:
    pass

total_supply: float = 0.0
accounts: list = []
try:
    total_supply = float(supply_result["value"]["uiAmount"] or 0)
    accounts = largest_accounts["value"] or []
except Exception:
    pass

if total_supply > 0 and accounts:
    amounts = [float(a.get("uiAmount") or 0) for a in accounts]
    top10_amount = sum(amounts[:10])
    result["top10_pct"] = top10_amount / total_supply * 100
    result["dev_pct"] = amounts[0] / total_supply * 100
    result["dev_address"] = accounts[0].get("address")
    result["whale_alert"] = any(amt / total_supply * 100 > 10.0 for amt in amounts)

return result
```

def get_pair_age_minutes(pair: dict):
created_at = pair.get(“pairCreatedAt”)
if not created_at:
return None
now_ms = time.time() * 1000
return (now_ms - float(created_at)) / 60_000

# ── Scoring ────────────────────────────────────────────────────────────────────

def score_token(volume, liquidity, change_24h, buy_pct, mint_revoked,
freeze_revoked, has_twitter, has_telegram, top10_pct,
dev_pct, age_min, whale_alert=False) -> int:
score = 0

```
if volume >= 200_000: score += 22
elif volume >= 100_000: score += 18
elif volume >= 50_000: score += 14
elif volume >= 30_000: score += 10
elif volume >= 10_000: score += 5

if liquidity >= 60_000: score += 18
elif liquidity >= 40_000: score += 14
elif liquidity >= 25_000: score += 10
elif liquidity >= 15_000: score += 7
elif liquidity >= 5_000: score += 4

if 62 <= buy_pct <= 72: score += 18
elif 58 <= buy_pct < 62 or 72 < buy_pct <= 78: score += 12
elif 55 <= buy_pct < 58 or 78 < buy_pct <= 85: score += 6

if change_24h >= 200: score += 14
elif change_24h >= 100: score += 11
elif change_24h >= 50: score += 8
elif change_24h >= 20: score += 5
elif change_24h > 0: score += 2

if mint_revoked: score += 8
if freeze_revoked: score += 6
if has_twitter: score += 4
if has_telegram: score += 4

if top10_pct is not None:
    if top10_pct <= 10: score += 8
    elif top10_pct <= 15: score += 4

if dev_pct is not None and dev_pct <= 3: score += 4

if age_min is not None:
    if 6 <= age_min <= 20: score += 4
    elif 4 <= age_min < 6 or 20 < age_min <= 30: score += 2

if whale_alert: score -= 10

return max(0, min(score, 100))
```

# ── Token processing ───────────────────────────────────────────────────────────

async def process_token(address, source, name, symbol, twitter, telegram, bot) -> None:
if address in seen_tokens:
return

```
stats["tokens_checked"] += 1
pair = await get_dexscreener_pair(address)
if not pair:
    stats["no_pair"] += 1
    return

name = pair.get("baseToken", {}).get("name", name) or name
symbol = pair.get("baseToken", {}).get("symbol", symbol) or symbol
mc = float(pair.get("marketCap", 0) or 0)
liquidity = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
volume = float((pair.get("volume") or {}).get("h24", 0) or 0)
change_24h = float((pair.get("priceChange") or {}).get("h24", 0) or 0)
price_usd = float(pair.get("priceUsd", 0) or 0)
chain = pair.get("chainId", "solana")

txns = pair.get("txns", {}).get("h24", {})
buys = int(txns.get("buys", 0) or 0)
sells = int(txns.get("sells", 0) or 0)
total_txns = buys + sells
buy_pct = (buys / total_txns * 100) if total_txns > 0 else 0

age_min = get_pair_age_minutes(pair)
has_twitter = bool(twitter)
has_telegram = bool(telegram)

# MC filter
if not (MIN_MC <= mc <= MAX_MC):
    stats["filter_mc"] += 1
    return
# Liquidity filter
if liquidity < MIN_LIQUIDITY:
    stats["filter_liquidity"] += 1
    return
# Volume filter
if volume < MIN_VOLUME:
    stats["filter_volume"] += 1
    return
# Buy pressure filter
if not (MIN_BUY_PRESSURE <= buy_pct <= MAX_BUY_PRESSURE):
    stats["filter_buy_pressure"] += 1
    return
# Price change filter
if change_24h <= 0:
    return
# Age filter
if age_min is not None and not (MIN_AGE_MIN <= age_min <= MAX_AGE_MIN):
    stats["filter_age"] += 1
    return

txns_5m_data = pair.get("txns", {}).get("m5", {})
txns_5m = int((txns_5m_data.get("buys") or 0) + (txns_5m_data.get("sells") or 0))

oc = await check_solana_on_chain(address)
mint_revoked = oc["mint_revoked"]
freeze_revoked = oc["freeze_revoked"]
top10_pct = oc["top10_pct"]
dev_pct = oc["dev_pct"]
whale_alert = oc["whale_alert"]
holder_count = oc["holder_count"]

if not mint_revoked:
    stats["filter_mint"] += 1
    return
if not freeze_revoked:
    stats["filter_freeze"] += 1
    return
if top10_pct is not None and top10_pct > MAX_TOP10_PCT:
    stats["filter_concentration"] += 1
    return
if dev_pct is not None and dev_pct > MAX_DEV_PCT:
    stats["filter_concentration"] += 1
    return

score = score_token(
    volume=volume, liquidity=liquidity, change_24h=change_24h,
    buy_pct=buy_pct, mint_revoked=mint_revoked, freeze_revoked=freeze_revoked,
    has_twitter=has_twitter, has_telegram=has_telegram,
    top10_pct=top10_pct, dev_pct=dev_pct, age_min=age_min,
    whale_alert=whale_alert,
)

if score < MIN_SCORE:
    stats["filter_score"] += 1
    return

seen_tokens.add(address)
stats["alerts_sent"] += 1

card = build_token_card(
    name=name, symbol=symbol, mc=mc, liquidity=liquidity,
    volume=volume, age_min=age_min or 0, buy_pct=buy_pct,
    mint_revoked=mint_revoked, freeze_revoked=freeze_revoked,
    top10_pct=top10_pct, dev_pct=dev_pct,
    holder_count=holder_count, whale_alert=whale_alert,
    txns_5m=txns_5m, score=score, address=address, chain=chain,
    source=source,
)

try:
    await bot.send_message(
        chat_id=CHAT_ID, text=card,
        parse_mode="Markdown", disable_web_page_preview=True
    )
    logger.info(f"Alert sent: {name} ({symbol}) score={score}")
except Exception as e:
    logger.error(f"Alert send failed: {e}")

# Track for PnL
_ensure_daily_reset()
tracked_tokens[address] = {
    "name": name, "symbol": symbol,
    "entry_mc": mc, "entry_price": price_usd,
    "alert_time": time.time(), "milestones_hit": set(),
}
daily_calls.append(
    {"address": address, "name": name, "symbol": symbol,
     "entry_mc": mc, "alert_time": time.time()}
)
```

async def fetch_dexscreener_profiles(bot) -> int:
try:
async with httpx.AsyncClient(timeout=10) as client:
resp = await client.get(DEXSCREENER_PROFILES_URL)
resp.raise_for_status()
profiles = resp.json() or []
except Exception as e:
logger.debug(f”Profiles fetch failed: {e}”)
return 0

```
solana = [p for p in profiles if p.get("chainId") == "solana" and p.get("tokenAddress")]
count = 0
for p in solana[:30]:
    address = p["tokenAddress"]
    if address in seen_tokens:
        continue
    links = p.get("links") or []
    twitter = next((l.get("url") for l in links if l.get("type") == "twitter"), None)
    telegram = next((l.get("url") for l in links if l.get("type") == "telegram"), None)
    await process_token(address, "DexScreener", p.get("description", "")[:20], "", twitter, telegram, bot)
    count += 1
return count
```

async def fetch_dexscreener_boosts(bot) -> int:
try:
async with httpx.AsyncClient(timeout=10) as client:
resp = await client.get(DEXSCREENER_BOOSTS_URL)
resp.raise_for_status()
profiles = resp.json() or []
except Exception as e:
logger.debug(f”Boosts fetch failed: {e}”)
return 0

```
solana = [p for p in profiles if p.get("chainId") == "solana" and p.get("tokenAddress")]
count = 0
for p in solana[:30]:
    address = p["tokenAddress"]
    if address in seen_tokens:
        continue
    links = p.get("links") or []
    twitter = next((l.get("url") for l in links if l.get("type") == "twitter"), None)
    telegram = next((l.get("url") for l in links if l.get("type") == "telegram"), None)
    await process_token(address, "DexBoosts", "", "", twitter, telegram, bot)
    count += 1
return count
```

async def fetch_pump_fun(bot) -> int:
try:
async with httpx.AsyncClient(timeout=10) as client:
resp = await client.get(PUMP_FUN_URL)
resp.raise_for_status()
coins = resp.json() or []
except Exception as e:
logger.debug(f”Pump.fun fetch failed: {e}”)
return 0

```
count = 0
for coin in coins[:20]:
    address = coin.get("mint")
    if not address or address in seen_tokens:
        continue
    twitter = coin.get("twitter")
    telegram = coin.get("telegram")
    name = coin.get("name", "")
    symbol = coin.get("symbol", "")
    await process_token(address, "Pump.fun", name, symbol, twitter, telegram, bot)
    count += 1
return count
```

async def monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
global cycle_count
cycle_count += 1
stats[“cycles”] += 1
bot = context.bot
await asyncio.gather(
fetch_dexscreener_profiles(bot),
fetch_dexscreener_boosts(bot),
fetch_pump_fun(bot),
return_exceptions=True,
)

async def track_price_job(context: ContextTypes.DEFAULT_TYPE) -> None:
if not tracked_tokens:
return
bot = context.bot
for address in list(tracked_tokens.keys()):
entry = tracked_tokens.get(address)
if not entry:
continue
try:
pair = await get_dexscreener_pair(address)
if not pair:
continue
current_mc = float(pair.get(“marketCap”, 0) or 0)
if current_mc <= 0 or entry[“entry_mc”] <= 0:
continue
multiple = current_mc / entry[“entry_mc”]
elapsed_min = int((time.time() - entry[“alert_time”]) / 60)

```
        for target in (2, 3, 4):
            label = f"{target}x"
            if multiple >= target and label not in entry["milestones_hit"]:
                entry["milestones_hit"].add(label)
                pnl_card = (
                    f"🏆 *PnL Update — {label} Hit!*\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"💎 {entry['name']} (${entry['symbol']})\n"
                    f"📊 Entry MC: {fmt_usd(entry['entry_mc'])}\n"
                    f"📈 Current MC: {fmt_usd(current_mc)}\n"
                    f"🚀 Multiple: `{multiple:.2f}x`\n"
                    f"⏱ Time to {label}: `{elapsed_min}m`\n"
                    f"🔗 [Dexscreener](https://dexscreener.com/solana/{address})"
                )
                await bot.send_message(
                    chat_id=CHAT_ID, text=pnl_card,
                    parse_mode="Markdown", disable_web_page_preview=True
                )

        if "4x" in entry["milestones_hit"] or (time.time() - entry["alert_time"]) > 21_600:
            tracked_tokens.pop(address, None)
    except Exception as e:
        logger.debug(f"Price track error: {e}")
```

def _ensure_daily_reset() -> None:
global daily_calls, daily_reset_date
today = datetime.date.today().isoformat()
if daily_reset_date != today:
daily_calls = []
daily_reset_date = today

async def midnight_reset_job(context: ContextTypes.DEFAULT_TYPE) -> None:
_ensure_daily_reset()

# ── Scan helper ────────────────────────────────────────────────────────────────

async def do_scan(address: str, reply_fn) -> None:
await reply_fn(f”🔍 Scanning `{address}`…”, parse_mode=“Markdown”)
pair, oc = await asyncio.gather(
get_dexscreener_pair(address),
check_solana_on_chain(address),
)
if not pair:
await reply_fn(“❌ No Solana token data found for that address.”)
return

```
name = pair.get("baseToken", {}).get("name", "Unknown")
symbol = pair.get("baseToken", {}).get("symbol", "???")
mc = float(pair.get("marketCap", 0) or 0)
liquidity = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
volume = float((pair.get("volume") or {}).get("h24", 0) or 0)
change_24h = float((pair.get("priceChange") or {}).get("h24", 0) or 0)
chain = pair.get("chainId", "solana")

txns_h24 = pair.get("txns", {}).get("h24", {})
buys = int(txns_h24.get("buys", 0) or 0)
sells = int(txns_h24.get("sells", 0) or 0)
total_txns = buys + sells
buy_pct = (buys / total_txns * 100) if total_txns > 0 else 0

txns_5m_data = pair.get("txns", {}).get("m5", {})
txns_5m = int((txns_5m_data.get("buys") or 0) + (txns_5m_data.get("sells") or 0))
age_min = get_pair_age_minutes(pair)

mint_revoked = oc["mint_revoked"]
freeze_revoked = oc["freeze_revoked"]
top10_pct = oc["top10_pct"]
dev_pct = oc["dev_pct"]
dev_address = oc["dev_address"]
whale_alert = oc["whale_alert"]
holder_count = oc["holder_count"]

info_socials = pair.get("info", {}).get("socials", []) or []
has_twitter = any(s.get("type") == "twitter" for s in info_socials)
has_telegram = any(s.get("type") == "telegram" for s in info_socials)

score = score_token(
    volume=volume, liquidity=liquidity, change_24h=change_24h,
    buy_pct=buy_pct, mint_revoked=mint_revoked, freeze_revoked=freeze_revoked,
    has_twitter=has_twitter, has_telegram=has_telegram,
    top10_pct=top10_pct, dev_pct=dev_pct, age_min=age_min,
    whale_alert=whale_alert,
)

card = build_token_card(
    name=name, symbol=symbol, mc=mc, liquidity=liquidity,
    volume=volume, age_min=age_min or 0, buy_pct=buy_pct,
    mint_revoked=mint_revoked, freeze_revoked=freeze_revoked,
    top10_pct=top10_pct, dev_pct=dev_pct,
    holder_count=holder_count, whale_alert=whale_alert,
    txns_5m=txns_5m, score=score, address=address, chain=chain,
)

vol_liq = volume / liquidity if liquidity > 0 else 0
chg_arrow = "🟢" if change_24h >= 0 else "🔴"
dev_addr_str = f"`{dev_address[:12]}...`" if dev_address else "❌"
extra = (
    f"\n━━━━━━━━━━━━━━━━━━━━\n"
    f"{chg_arrow} 24h: `{change_24h:+.1f}%` | Vol/Liq: `{vol_liq:.1f}x`\n"
    f"🟢 Buys: `{buys:,}` | 🔴 Sells: `{sells:,}`\n"
    f"👨‍💻 Dev addr: {dev_addr_str}"
)

await reply_fn(card + extra, parse_mode="Markdown", disable_web_page_preview=True)
```

# ── Commands ───────────────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
await update.message.reply_text(
“💎 *GemStalker is active*\n\n”
“Just paste any Solana CA to scan it instantly.\n\n”
“Commands:\n”
“/scan <address> — scan a token\n”
“/calls — today’s alert log\n”
“/status — scanner stats\n”
“/help — all commands”,
parse_mode=“Markdown”
)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
await update.message.reply_text(
“💎 *GemStalker Commands*\n\n”
“/scan <address> — scan any Solana token\n”
“/calls — today’s alerted tokens with multiples\n”
“/status — live scanner stats\n”
“/start — show intro\n\n”
f”Auto-scanner runs every 20s from Pump.fun + DexScreener.\n”
f”Filters: MC {fmt_usd(MIN_MC)}–{fmt_usd(MAX_MC)} | “
f”Liq {fmt_usd(MIN_LIQUIDITY)} | Vol {fmt_usd(MIN_VOLUME)} | “
f”Buys {MIN_BUY_PRESSURE:.0f}–{MAX_BUY_PRESSURE:.0f}% | “
f”Score {MIN_SCORE}/100”,
parse_mode=“Markdown”
)

async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
if not context.args:
await update.message.reply_text(“Usage: /scan <contract_address>”)
return
address = context.args[0].strip()
await do_scan(address, update.message.reply_text)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
uptime_secs = int(time.time() - start_time) if start_time else 0
hours, rem = divmod(uptime_secs, 3600)
mins, secs = divmod(rem, 60)
uptime_str = f”{hours}h {mins}m {secs}s”
total_filtered = sum(stats[k] for k in stats if k.startswith(“filter_”))

```
msg = (
    f"📊 *GemStalker Status*\n"
    f"━━━━━━━━━━━━━━━━━━━━\n"
    f"⏱ Uptime: `{uptime_str}`\n"
    f"🔄 Cycles: `{stats['cycles']}`\n"
    f"🔍 Checked: `{stats['tokens_checked']}`\n"
    f"🚨 Alerts: `{stats['alerts_sent']}`\n"
    f"👀 Seen: `{len(seen_tokens)}`\n"
    f"━━━━━━━━━━━━━━━━━━━━\n"
    f"*Rejections* ({total_filtered} total)\n"
    f"💰 MC: `{stats['filter_mc']}`\n"
    f"💧 Liquidity: `{stats['filter_liquidity']}`\n"
    f"📊 Volume: `{stats['filter_volume']}`\n"
    f"🟢 Buy pressure: `{stats['filter_buy_pressure']}`\n"
    f"🕐 Age: `{stats['filter_age']}`\n"
    f"👥 Concentration: `{stats['filter_concentration']}`\n"
    f"🔒 Mint: `{stats['filter_mint']}`\n"
    f"🎯 Score: `{stats['filter_score']}`"
)
await update.message.reply_text(msg, parse_mode="Markdown")
```

async def calls_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
_ensure_daily_reset()
if not daily_calls:
await update.message.reply_text(“📭 No calls today yet.”)
return

```
lines = []
hit_2x = 0
for i, call in enumerate(daily_calls, 1):
    address = call["address"]
    entry_mc = call["entry_mc"]
    alert_dt = datetime.datetime.fromtimestamp(call["alert_time"]).strftime("%H:%M")
    current_mc = None
    try:
        pair = await get_dexscreener_pair(address)
        if pair:
            current_mc = float(pair.get("marketCap", 0) or 0)
    except Exception:
        pass

    if current_mc and entry_mc > 0:
        multiple = current_mc / entry_mc
        multiple_str = f"`{multiple:.2f}x`"
        if multiple >= 2.0:
            hit_2x += 1
    else:
        multiple_str = "`N/A`"

    lines.append(
        f"{i}. *{call['name']}* (${call['symbol']}) — {alert_dt}\n"
        f"   Entry: {fmt_usd(entry_mc)} → {multiple_str}"
    )

today_str = datetime.date.today().strftime("%b %d")
header = (
    f"📋 *Calls — {today_str}*\n"
    f"Total: `{len(daily_calls)}` | 2x+: `{hit_2x}`\n"
    f"━━━━━━━━━━━━━━━━━━━━\n"
)
await update.message.reply_text(
    header + "\n".join(lines),
    parse_mode="Markdown",
    disable_web_page_preview=True,
)
```

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
text = (update.message.text or “”).strip()
if is_solana_address(text):
await do_scan(text, update.message.reply_text)
else:
await update.message.reply_text(“💎 Paste a Solana CA to scan it, or use /help”)

# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
global start_time, daily_reset_date
start_time = time.time()
_ensure_daily_reset()

```
# Start Flask in background thread
flask_thread = threading.Thread(target=run_flask, daemon=True)
flask_thread.start()
logger.info("Flask keep-alive server started")

app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("help", help_command))
app.add_handler(CommandHandler("scan", scan_command))
app.add_handler(CommandHandler("status", status_command))
app.add_handler(CommandHandler("calls", calls_command))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

jq = app.job_queue
jq.run_repeating(monitor_job, interval=20, first=10)
jq.run_repeating(track_price_job, interval=120, first=60)
jq.run_repeating(midnight_reset_job, interval=3600, first=60)

logger.info("GemStalker running 💎")
app.run_polling(allowed_updates=Update.ALL_TYPES)
```

if **name** == “**main**”:
main()
