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
filters,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(**name**)

TOKEN = os.getenv(“TELEGRAM_BOT_TOKEN”)
CHAT_ID = os.getenv(“CHAT_ID”)

if not TOKEN:
raise ValueError(“Missing TELEGRAM_BOT_TOKEN”)

DEX_PROFILES = “https://api.dexscreener.com/token-profiles/latest/v1”
DEX_BOOSTS = “https://api.dexscreener.com/token-boosts/latest/v1”
DEX_TOKEN = “https://api.dexscreener.com/latest/dex/tokens/”
PUMP_FUN = “https://frontend-api.pump.fun/coins?limit=20&sort=created_timestamp&order=DESC”
SOL_RPC = “https://api.mainnet-beta.solana.com”

MIN_MC = 25_000
MAX_MC = 200_000
MIN_LIQ = 15_000
MIN_VOL = 30_000
MIN_SCORE = 70
MIN_BUY = 60.0
MAX_BUY = 72.0
MIN_AGE = 4
MAX_AGE = 30
MAX_TOP10 = 18.0
MAX_DEV = 3.0

seen_tokens = set()
tracked_tokens = {}
daily_calls = deque(maxlen=500)
start_time = time.time()

stats = {
“cycles”: 0,
“checked”: 0,
“alerts”: 0,
“filter_mc”: 0,
“filter_liq”: 0,
“filter_vol”: 0,
“filter_buy”: 0,
“filter_age”: 0,
“filter_score”: 0,
}

http = httpx.AsyncClient(
timeout=10,
limits=httpx.Limits(max_connections=20)
)

flask_app = Flask(**name**)

@flask_app.route(”/”)
def health():
return {
“status”: “alive”,
“cycles”: stats[“cycles”],
“alerts”: stats[“alerts”],
}

def run_flask():
port = int(os.getenv(“PORT”, 8080))
flask_app.run(host=“0.0.0.0”, port=port)

def is_solana_address(text):
return bool(re.fullmatch(r”[1-9A-HJ-NP-Za-km-z]{32,44}”, text.strip()))

def fmt_usd(v):
try:
v = float(v)
if v >= 1_000_000:
return f”${v/1_000_000:.2f}M”
if v >= 1_000:
return f”${v/1_000:.1f}K”
return f”${v:,.0f}”
except:
return “N/A”

def get_age(pair):
created = pair.get(“pairCreatedAt”)
if not created:
return None
return (time.time() * 1000 - float(created)) / 60_000

def potential(score, mc):
if mc < 40_000 and score >= 90:
return “10x”
if mc < 100_000 and score >= 85:
return “5x”
return “2x”

async def get_pair(address):
try:
r = await http.get(f”{DEX_TOKEN}{address}”)
pairs = r.json().get(“pairs”, [])
sol = [p for p in pairs if p.get(“chainId”) == “solana”]
if not sol:
return None
return max(sol, key=lambda p: float(p.get(“liquidity”, {}).get(“usd”, 0) or 0))
except Exception as e:
logger.debug(f”Pair error: {e}”)
return None

async def rpc_call(method, params):
try:
r = await http.post(SOL_RPC, json={
“jsonrpc”: “2.0”, “id”: 1,
“method”: method, “params”: params
}, timeout=12)
return r.json().get(“result”)
except Exception as e:
logger.debug(f”RPC error: {e}”)
return None

async def check_chain(address):
result = {
“mint_revoked”: True,
“freeze_revoked”: True,
“top10_pct”: None,
“dev_pct”: None,
“whale_alert”: False,
}
try:
account, supply, largest = await asyncio.gather(
rpc_call(“getAccountInfo”, [address, {“encoding”: “jsonParsed”}]),
rpc_call(“getTokenSupply”, [address]),
rpc_call(“getTokenLargestAccounts”, [address]),
)
info = account[“value”][“data”][“parsed”][“info”]
result[“mint_revoked”] = info.get(“mintAuthority”) is None
result[“freeze_revoked”] = info.get(“freezeAuthority”) is None
total = float(supply[“value”][“uiAmount”] or 0)
wallets = largest[“value”]
if total > 0 and wallets:
amounts = [float(x.get(“uiAmount”) or 0) for x in wallets]
result[“top10_pct”] = sum(amounts[:10]) / total * 100
result[“dev_pct”] = amounts[0] / total * 100
result[“whale_alert”] = any(a / total * 100 > 15 for a in amounts)
except Exception as e:
logger.debug(f”Chain error: {e}”)
return result

def score_token(vol, liq, buy_pct, change, mint_ok, freeze_ok, top10, dev, age, whale):
s = 0
if vol > 200_000: s += 22
elif vol > 100_000: s += 18
elif vol > 50_000: s += 14
elif vol > 30_000: s += 10
if liq > 60_000: s += 18
elif liq > 40_000: s += 14
elif liq > 25_000: s += 10
elif liq > 15_000: s += 7
if 62 <= buy_pct <= 72: s += 18
elif 58 <= buy_pct < 62 or 72 < buy_pct <= 78: s += 10
if change > 200: s += 14
elif change > 100: s += 10
elif change > 50: s += 7
elif change > 0: s += 3
if mint_ok: s += 8
if freeze_ok: s += 6
if top10 is not None:
if top10 <= 10: s += 8
elif top10 <= 15: s += 4
if dev is not None and dev <= 3: s += 4
if age is not None and 6 <= age <= 20: s += 4
if whale: s -= 10
return max(0, min(s, 100))

def build_card(name, symbol, mc, liq, vol, buy_pct, age, mint_ok, freeze_ok, top10, dev, score, address, source):
top10_str = f”{top10:.1f}%” if top10 is not None else “N/A”
dev_str = f”{dev:.1f}%” if dev is not None else “N/A”
age_str = f”{age:.0f}m” if age is not None else “N/A”
pot = potential(score, mc)
link = f”https://dexscreener.com/solana/{address}”
return (
f”<b>GEMSTALKER ALERT</b>\n\n”
f”<b>{name}</b> (${symbol})\n”
f”Source: {source}\n\n”
f”MC: {fmt_usd(mc)} | Liq: {fmt_usd(liq)}\n”
f”Vol: {fmt_usd(vol)} | Age: {age_str}\n”
f”Buys: {buy_pct:.0f}%\n”
f”Mint: {‘OK’ if mint_ok else ‘NO’} | Freeze: {‘OK’ if freeze_ok else ‘NO’}\n”
f”Top10: {top10_str} | Dev: {dev_str}\n\n”
f”Score: {score}/100\n”
f”Potential: {pot}\n\n”
f”<a href='{link}'>View on DexScreener</a>”
)

async def process_token(address, source, name, symbol, bot):
if address in seen_tokens:
return
stats[“checked”] += 1
pair = await get_pair(address)
if not pair:
return
name = pair.get(“baseToken”, {}).get(“name”, name) or name
symbol = pair.get(“baseToken”, {}).get(“symbol”, symbol) or symbol
mc = float(pair.get(“marketCap”, 0) or 0)
liq = float(pair.get(“liquidity”, {}).get(“usd”, 0) or 0)
vol = float(pair.get(“volume”, {}).get(“h24”, 0) or 0)
change = float(pair.get(“priceChange”, {}).get(“h24”, 0) or 0)
txns = pair.get(“txns”, {}).get(“h24”, {})
buys = int(txns.get(“buys”, 0) or 0)
sells = int(txns.get(“sells”, 0) or 0)
total = buys + sells
buy_pct = buys / total * 100 if total > 0 else 0
age = get_age(pair)
if mc < MIN_MC or mc > MAX_MC:
stats[“filter_mc”] += 1
return
if liq < MIN_LIQ:
stats[“filter_liq”] += 1
return
if vol < MIN_VOL:
stats[“filter_vol”] += 1
return
if not (MIN_BUY <= buy_pct <= MAX_BUY):
stats[“filter_buy”] += 1
return
if change <= 0:
return
if age is not None and not (MIN_AGE <= age <= MAX_AGE):
stats[“filter_age”] += 1
return
chain = await check_chain(address)
if not chain[“mint_revoked”] or not chain[“freeze_revoked”]:
return
top10 = chain[“top10_pct”]
dev = chain[“dev_pct”]
if top10 is not None and top10 > MAX_TOP10:
return
if dev is not None and dev > MAX_DEV:
return
score = score_token(vol, liq, buy_pct, change,
chain[“mint_revoked”], chain[“freeze_revoked”],
top10, dev, age, chain[“whale_alert”])
if score < MIN_SCORE:
stats[“filter_score”] += 1
return
seen_tokens.add(address)
stats[“alerts”] += 1
card = build_card(name, symbol, mc, liq, vol, buy_pct, age,
chain[“mint_revoked”], chain[“freeze_revoked”],
top10, dev, score, address, source)
try:
await bot.send_message(
chat_id=CHAT_ID, text=card,
parse_mode=“HTML”, disable_web_page_preview=True
)
logger.info(f”ALERT: {symbol} score={score}”)
except Exception as e:
logger.error(f”Send error: {e}”)
tracked_tokens[address] = {
“name”: name, “symbol”: symbol,
“entry_mc”: mc, “alert_time”: time.time(),
“milestones”: set()
}
daily_calls.append({
“address”: address, “name”: name,
“symbol”: symbol, “entry_mc”: mc,
“alert_time”: time.time()
})

async def monitor_job(context):
stats[“cycles”] += 1
bot = context.bot
try:
r = await http.get(DEX_PROFILES)
for p in r.json():
if p.get(“chainId”) != “solana”:
continue
addr = p.get(“tokenAddress”)
if addr and addr not in seen_tokens:
await process_token(addr, “DexScreener”, “”, “”, bot)
except Exception as e:
logger.error(f”Profiles error: {e}”)
try:
r = await http.get(DEX_BOOSTS)
for p in r.json():
if p.get(“chainId”) != “solana”:
continue
addr = p.get(“tokenAddress”)
if addr and addr not in seen_tokens:
await process_token(addr, “DexBoosts”, “”, “”, bot)
except Exception as e:
logger.error(f”Boosts error: {e}”)
try:
r = await http.get(PUMP_FUN)
for c in r.json():
addr = c.get(“mint”)
if addr and addr not in seen_tokens:
await process_token(addr, “Pump.fun”, c.get(“name”,””), c.get(“symbol”,””), bot)
except Exception as e:
logger.error(f”Pump error: {e}”)

async def track_job(context):
if not tracked_tokens:
return
bot = context.bot
for addr in list(tracked_tokens.keys()):
entry = tracked_tokens.get(addr)
if not entry:
continue
try:
pair = await get_pair(addr)
if not pair:
continue
cur_mc = float(pair.get(“marketCap”, 0) or 0)
if cur_mc <= 0 or entry[“entry_mc”] <= 0:
continue
mult = cur_mc / entry[“entry_mc”]
elapsed = int((time.time() - entry[“alert_time”]) / 60)
for target in (2, 3, 4):
label = f”{target}x”
if mult >= target and label not in entry[“milestones”]:
entry[“milestones”].add(label)
msg = (
f”<b>PNL - {label} HIT!</b>\n\n”
f”{entry[‘name’]} (${entry[‘symbol’]})\n”
f”Entry: {fmt_usd(entry[‘entry_mc’])}\n”
f”Now: {fmt_usd(cur_mc)}\n”
f”Multiple: {mult:.2f}x\n”
f”Time: {elapsed}m\n\n”
f”<a href='https://dexscreener.com/solana/{addr}'>Chart</a>”
)
await bot.send_message(
chat_id=CHAT_ID, text=msg,
parse_mode=“HTML”, disable_web_page_preview=True
)
if “4x” in entry[“milestones”] or (time.time() - entry[“alert_time”]) > 21600:
tracked_tokens.pop(addr, None)
except Exception as e:
logger.debug(f”Track error: {e}”)

async def do_scan(address, reply_fn):
await reply_fn(“Scanning…”)
pair = await get_pair(address)
if not pair:
await reply_fn(“No data found.”)
return
chain = await check_chain(address)
name = pair.get(“baseToken”, {}).get(“name”, “Unknown”)
symbol = pair.get(“baseToken”, {}).get(“symbol”, “???”)
mc = float(pair.get(“marketCap”, 0) or 0)
liq = float(pair.get(“liquidity”, {}).get(“usd”, 0) or 0)
vol = float(pair.get(“volume”, {}).get(“h24”, 0) or 0)
change = float(pair.get(“priceChange”, {}).get(“h24”, 0) or 0)
txns = pair.get(“txns”, {}).get(“h24”, {})
buys = int(txns.get(“buys”, 0) or 0)
sells = int(txns.get(“sells”, 0) or 0)
total = buys + sells
buy_pct = buys / total * 100 if total > 0 else 0
age = get_age(pair)
score = score_token(vol, liq, buy_pct, change,
chain[“mint_revoked”], chain[“freeze_revoked”],
chain[“top10_pct”], chain[“dev_pct”],
age, chain[“whale_alert”])
card = build_card(name, symbol, mc, liq, vol, buy_pct, age,
chain[“mint_revoked”], chain[“freeze_revoked”],
chain[“top10_pct”], chain[“dev_pct”],
score, address, “Manual Scan”)
await reply_fn(card, parse_mode=“HTML”, disable_web_page_preview=True)

async def start_cmd(update, context):
await update.message.reply_text(
“GemStalker online.\n\n”
“Paste any Solana CA to scan.\n\n”
“/scan address\n”
“/calls - today calls\n”
“/status - bot stats”
)

async def scan_cmd(update, context):
if not context.args:
await update.message.reply_text(“Usage: /scan address”)
return
await do_scan(context.args[0], update.message.reply_text)

async def status_cmd(update, context):
uptime = int(time.time() - start_time)
h, r = divmod(uptime, 3600)
m, s = divmod(r, 60)
msg = (
f”GemStalker Status\n\n”
f”Uptime: {h}h {m}m {s}s\n”
f”Cycles: {stats[‘cycles’]}\n”
f”Checked: {stats[‘checked’]}\n”
f”Alerts: {stats[‘alerts’]}\n”
f”Tracking: {len(tracked_tokens)}\n\n”
f”Rejections:\n”
f”MC: {stats[‘filter_mc’]}\n”
f”Liq: {stats[‘filter_liq’]}\n”
f”Vol: {stats[‘filter_vol’]}\n”
f”Buy: {stats[‘filter_buy’]}\n”
f”Age: {stats[‘filter_age’]}\n”
f”Score: {stats[‘filter_score’]}”
)
await update.message.reply_text(msg)

async def calls_cmd(update, context):
if not daily_calls:
await update.message.reply_text(“No calls yet today.”)
return
lines = []
hit2x = 0
for i, c in enumerate(list(daily_calls)[-20:], 1):
try:
pair = await get_pair(c[“address”])
cur = float(pair.get(“marketCap”, 0) or 0) if pair else 0
mult = cur / c[“entry_mc”] if c[“entry_mc”] > 0 and cur > 0 else 0
mult_str = f”{mult:.2f}x”
if mult >= 2:
hit2x += 1
except:
mult_str = “N/A”
t = datetime.datetime.fromtimestamp(c[“alert_time”]).strftime(”%H:%M”)
lines.append(f”{i}. {c[‘name’]} (${c[‘symbol’]}) {t} - {mult_str}”)
header = f”Calls: {len(daily_calls)} | 2x+: {hit2x}\n\n”
await update.message.reply_text(header + “\n”.join(lines))

async def msg_handler(update, context):
text = (update.message.text or “”).strip()
if is_solana_address(text):
await do_scan(text, update.message.reply_text)
else:
await update.message.reply_text(“Paste a Solana CA to scan.”)

def main():
threading.Thread(target=run_flask, daemon=True).start()
logger.info(“Flask started”)
app = Application.builder().token(TOKEN).build()
app.add_handler(CommandHandler(“start”, start_cmd))
app.add_handler(CommandHandler(“scan”, scan_cmd))
app.add_handler(CommandHandler(“status”, status_cmd))
app.add_handler(CommandHandler(“calls”, calls_cmd))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_handler))
jq = app.job_queue
jq.run_repeating(monitor_job, interval=20, first=5)
jq.run_repeating(track_job, interval=120, first=60)
logger.info(“GemStalker running”)
app.run_polling()

if **name** == “**main**”:
main()
