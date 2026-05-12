import os
import re
import io
import time
import json
import asyncio
import logging
import datetime
import threading
from collections import deque
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance

import httpx
from flask import Flask
from telegram import Update
from telegram.ext import (
Application,
CommandHandler,
MessageHandler,
ConversationHandler,
ContextTypes,
filters,
)

# ── logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
format=”%(asctime)s | %(levelname)s | %(message)s”,
level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ── env ───────────────────────────────────────────────────────────────────────

TOKEN   = os.getenv(“TELEGRAM_BOT_TOKEN”)
CHAT_ID = os.getenv(“CHAT_ID”)

# ── api endpoints ─────────────────────────────────────────────────────────────

DEX_PROFILES  = “https://api.dexscreener.com/token-profiles/latest/v1”
DEX_BOOSTS    = “https://api.dexscreener.com/token-boosts/latest/v1”
DEX_TOKEN     = “https://api.dexscreener.com/latest/dex/tokens/”
PUMP_COINS    = “https://frontend-api.pump.fun/coins?limit=50&sort=created_timestamp&order=DESC”
PUMP_TOKEN    = “https://frontend-api.pump.fun/coins/”
SOLSCAN_TOKEN = “https://public-api.solscan.io/token/holders?tokenAddress=”
SOLSCAN_META  = “https://public-api.solscan.io/token/meta?tokenAddress=”
SOL_RPC       = “https://api.mainnet-beta.solana.com”

# ── filters (from your PDF) ───────────────────────────────────────────────────

F_MC_MIN      = 5_000
F_MC_MAX      = 50_000
F_LP_MIN      = 4_000
F_TOP10_MAX   = 35       # %
F_DEV_MAX     = 1        # %
F_VOL_MIN     = 6_000
F_HOLDERS_MIN = 20
F_FEES_MIN    = 0.1      # SOL
F_REQUIRE_SOC = True

# ── pump milestones for re-alert ──────────────────────────────────────────────

PUMP_MILESTONES = [2, 5, 10, 25, 50, 100]

# ── conversation state ────────────────────────────────────────────────────────

WAIT_PHOTO = 1

# ── shared state ──────────────────────────────────────────────────────────────

http           = httpx.AsyncClient(timeout=15)
seen_tokens    : dict  = {}
call_history   : deque = deque(maxlen=500)
pnl_pending    : dict  = {}
bot_start_time : float = time.time()
flask_app              = Flask(__name__)

# ════════════════════════════════════════════════════════════════════════════

# FLASK

# ════════════════════════════════════════════════════════════════════════════

@flask_app.route(”/”)
def health():
return {“status”: “alive”, “calls”: len(call_history)}

def run_flask():
flask_app.run(host=“0.0.0.0”, port=int(os.getenv(“PORT”, 8080)))

# ════════════════════════════════════════════════════════════════════════════

# HELPERS

# ════════════════════════════════════════════════════════════════════════════

def fmt(n: float, decimals: int = 2) -> str:
if n is None:
return “?”
if n >= 1_000_000_000:
return f”{n/1_000_000_000:.{decimals}f}B”
if n >= 1_000_000:
return f”{n/1_000_000:.{decimals}f}M”
if n >= 1_000:
return f”{n/1_000:.{decimals}f}K”
return f”{n:.{decimals}f}”

def age_str(created_ts_ms) -> str:
if not created_ts_ms:
return “?”
secs = time.time() - created_ts_ms / 1000
if secs < 60:   return f”{int(secs)}s”
if secs < 3600: return f”{int(secs/60)}m”
if secs < 86400:return f”{secs/3600:.1f}h”
return f”{secs/86400:.1f}d”

def since_str(ts: float) -> str:
return age_str(ts * 1000)

async def fetch(url: str, json_body: dict = None):
try:
if json_body:
r = await http.post(url, json=json_body)
else:
r = await http.get(url)
r.raise_for_status()
return r.json()
except Exception as e:
logger.debug(f”fetch failed {url}: {e}”)
return None

# ════════════════════════════════════════════════════════════════════════════

# DATA FETCHING

# ════════════════════════════════════════════════════════════════════════════

async def get_dex_data(address: str):
data = await fetch(DEX_TOKEN + address)
if not data:
return None
pairs = data.get(“pairs”) or []
if not pairs:
return None
p    = pairs[0]
base = p.get(“baseToken”, {})
info = p.get(“info”, {})
return {
“address”  : address,
“name”     : base.get(“name”, “Unknown”),
“symbol”   : base.get(“symbol”, “?”),
“price”    : float(p.get(“priceUsd”) or 0),
“mc”       : float(p.get(“fdv”) or 0),
“lp”       : float((p.get(“liquidity”) or {}).get(“usd”) or 0),
“vol_5m”   : float((p.get(“volume”) or {}).get(“m5”) or 0),
“vol_1h”   : float((p.get(“volume”) or {}).get(“h1”) or 0),
“vol_24h”  : float((p.get(“volume”) or {}).get(“h24”) or 0),
“buys_5m”  : int((p.get(“txns”) or {}).get(“m5”, {}).get(“buys”) or 0),
“sells_5m” : int((p.get(“txns”) or {}).get(“m5”, {}).get(“sells”) or 0),
“buys_1h”  : int((p.get(“txns”) or {}).get(“h1”, {}).get(“buys”) or 0),
“sells_1h” : int((p.get(“txns”) or {}).get(“h1”, {}).get(“sells”) or 0),
“chain”    : p.get(“chainId”, “solana”),
“dex”      : p.get(“dexId”, “?”),
“pair_age” : age_str(p.get(“pairCreatedAt”)),
“pair_ts”  : p.get(“pairCreatedAt”),
“url”      : p.get(“url”, “”),
“socials”  : info.get(“socials”, []),
“websites” : info.get(“websites”, []),
“has_social”: bool(info.get(“socials”) or info.get(“websites”)),
“dex_paid” : bool(info.get(“header”) or info.get(“openGraph”)),
“price_1h” : float((p.get(“priceChange”) or {}).get(“h1”) or 0),
“price_24h”: float((p.get(“priceChange”) or {}).get(“h24”) or 0),
}

async def get_pump_data(address: str):
data = await fetch(PUMP_TOKEN + address)
if not data:
return None
return {
“migration”    : data.get(“raydium_pool”) is not None,
“bonding_curve”: float(data.get(“bonding_curve_percentage”) or 0),
“dev_holding”  : float(data.get(“creator_percentage”) or 0),
“total_supply” : float(data.get(“total_supply”) or 0),
“twitter”      : data.get(“twitter”, “”),
“telegram”     : data.get(“telegram”, “”),
“website”      : data.get(“website”, “”),
}

async def get_holders(address: str) -> dict:
result = {“top10_pct”: None, “holder_count”: None, “top_holders”: []}
data = await fetch(f”{SOLSCAN_TOKEN}{address}&limit=10&offset=0”)
if not data:
return result
holders = data.get(“data”, [])
total   = data.get(“total”)
if not holders:
return result
supply_data = await fetch(f”{SOLSCAN_META}{address}”)
supply = float((supply_data or {}).get(“supply”) or 0)
if supply:
top10_sum = sum(float(h.get(“amount”) or 0) for h in holders)
result[“top10_pct”]   = round((top10_sum / supply) * 100, 1)
result[“top_holders”] = [
round((float(h.get(“amount”) or 0) / supply) * 100, 2)
for h in holders[:5]
]
result[“holder_count”] = total
return result

async def get_fees_paid(address: str) -> float:
try:
payload = {
“jsonrpc”: “2.0”, “id”: 1,
“method”: “getSignaturesForAddress”,
“params”: [address, {“limit”: 10}],
}
data = await fetch(SOL_RPC, json_body=payload)
if not data:
return 0.0
total = 0.0
for sig_info in (data.get(“result”) or [])[:5]:
sig = sig_info.get(“signature”)
tx  = await fetch(SOL_RPC, json_body={
“jsonrpc”: “2.0”, “id”: 1,
“method”: “getTransaction”,
“params”: [sig, {“encoding”: “json”, “maxSupportedTransactionVersion”: 0}],
})
if tx and tx.get(“result”):
total += tx[“result”].get(“meta”, {}).get(“fee”, 0) / 1e9
return round(total, 4)
except Exception as e:
logger.debug(f”get_fees_paid: {e}”)
return 0.0

async def get_smart_wallets(address: str) -> int:
data = await fetch(f”{SOLSCAN_TOKEN}{address}&limit=20&offset=0”)
if not data:
return 0
holders     = data.get(“data”, [])
supply_data = await fetch(f”{SOLSCAN_META}{address}”)
supply      = float((supply_data or {}).get(“supply”) or 0)
if not supply:
return 0
return sum(1 for h in holders if (float(h.get(“amount”) or 0) / supply) * 100 >= 1)

# ════════════════════════════════════════════════════════════════════════════

# SCORING

# ════════════════════════════════════════════════════════════════════════════

def score_token(dex: dict, pump, holders: dict) -> tuple:
score = 0
notes = []

```
mc = dex["mc"]
if F_MC_MIN <= mc <= F_MC_MAX:
    score += 20
    notes.append(f"✅ MC ${fmt(mc)} — in sweet spot ($5k–$50k)")
elif mc < F_MC_MIN:
    notes.append(f"⚠️ MC ${fmt(mc)} — too low, higher risk of rug")
else:
    notes.append(f"⚠️ MC ${fmt(mc)} — above $50k, early edge gone")

if dex["lp"] >= F_LP_MIN:
    score += 15
    notes.append(f"✅ LP ${fmt(dex['lp'])} — solid backing")
else:
    notes.append(f"❌ LP ${fmt(dex['lp'])} — thin, easy to manipulate")

b, s = dex["buys_5m"], dex["sells_5m"]
if b > s:
    ratio = b / max(s, 1)
    pts   = min(int(ratio * 4), 10)
    score += pts
    notes.append(f"✅ Buy pressure {b}B/{s}S — accumulation in play")
else:
    notes.append(f"⚠️ Sell pressure {b}B/{s}S — distribution risk")

if dex["vol_5m"] >= 1000:
    score += 10
    notes.append(f"✅ 5m vol ${fmt(dex['vol_5m'])} — active trading")
else:
    notes.append(f"👀 Low 5m vol ${fmt(dex['vol_5m'])} — quiet, watch for breakout")

top10 = holders.get("top10_pct")
if top10 is not None:
    if top10 <= F_TOP10_MAX:
        score += 15
        notes.append(f"✅ Top10 {top10}% — distributed supply")
    elif top10 <= 50:
        score += 5
        notes.append(f"⚠️ Top10 {top10}% — somewhat concentrated")
    else:
        notes.append(f"❌ Top10 {top10}% — whale risk, concentrated")

hc = holders.get("holder_count")
if hc:
    if hc >= 100:
        score += 5
        notes.append(f"✅ {hc} holders — good distribution")
    elif hc >= F_HOLDERS_MIN:
        score += 2
        notes.append(f"👀 {hc} holders — early stage")
    else:
        notes.append(f"❌ Only {hc} holders — very early / risky")

if pump and pump.get("migration"):
    score += 10
    notes.append("✅ Migrated from pump.fun — survived bonding curve")
elif pump:
    bc = pump.get("bonding_curve", 0)
    notes.append(f"👀 Still on pump.fun — {bc:.0f}% bonded")

has_soc = dex["has_social"] or (pump and any([pump.get("twitter"), pump.get("telegram"), pump.get("website")]))
if has_soc:
    score += 5
    notes.append("✅ Has socials — team is visible")
else:
    notes.append("❌ No socials — anon dev, higher risk")

dev_h = pump.get("dev_holding", 999) if pump else 999
if dev_h <= F_DEV_MAX:
    score += 10
    notes.append(f"✅ Dev holding {dev_h:.1f}% — not a threat")
elif dev_h <= 5:
    notes.append(f"⚠️ Dev holding {dev_h:.1f}% — monitor")
else:
    notes.append(f"❌ Dev holding {dev_h:.1f}% — dump risk")

return min(score, 100), notes
```

# ════════════════════════════════════════════════════════════════════════════

# FILTER CHECK

# ════════════════════════════════════════════════════════════════════════════

def passes_filters(dex: dict, pump, holders: dict, fees: float) -> tuple:
mc    = dex[“mc”]
lp    = dex[“lp”]
top10 = holders.get(“top10_pct”)
hc    = holders.get(“holder_count”) or 0
dev_h = pump.get(“dev_holding”) if pump else None

```
if not (F_MC_MIN <= mc <= F_MC_MAX):
    return False, f"MC ${fmt(mc)} out of $5k–$50k range"
if lp < F_LP_MIN:
    return False, f"LP ${fmt(lp)} below ${fmt(F_LP_MIN)}"
if top10 is not None and top10 > F_TOP10_MAX:
    return False, f"Top10 {top10}% exceeds {F_TOP10_MAX}%"
if hc < F_HOLDERS_MIN:
    return False, f"Only {hc} holders (min {F_HOLDERS_MIN})"
if dev_h is not None and dev_h > F_DEV_MAX:
    return False, f"Dev holding {dev_h:.1f}% > {F_DEV_MAX}%"
if dex["vol_24h"] < F_VOL_MIN:
    return False, f"24h vol ${fmt(dex['vol_24h'])} below ${fmt(F_VOL_MIN)}"
if F_REQUIRE_SOC:
    has_soc = dex["has_social"] or (pump and any([
        pump.get("twitter"), pump.get("telegram"), pump.get("website")
    ]))
    if not has_soc:
        return False, "No socials found"
return True, "ok"
```

# ════════════════════════════════════════════════════════════════════════════

# ALERT FORMAT

# ════════════════════════════════════════════════════════════════════════════

def build_alert(dex: dict, pump, holders: dict, fees: float, score: int, tag: str = “🔥 HIGH SCORE CALL”) -> str:
addr     = dex[“address”]
top10    = holders.get(“top10_pct”, “?”)
top_h    = holders.get(“top_holders”, [])
hcount   = holders.get(“holder_count”, “?”)
dev_h    = f”{pump.get(‘dev_holding’, ‘?’):.1f}” if pump and pump.get(“dev_holding”) is not None else “?”
migrated = pump.get(“migration”, False) if pump else False
bc       = pump.get(“bonding_curve”, 0) if pump else 0
twitter  = (pump.get(“twitter”) if pump else “”) or “”
tg       = (pump.get(“telegram”) if pump else “”) or “”
web      = (pump.get(“website”) if pump else “”) or “”

```
soc_parts = []
if twitter: soc_parts.append(f"[X]({twitter})")
for s in dex.get("socials", []):
    label = s.get("type", "link").capitalize()
    url   = s.get("url", "")
    if url: soc_parts.append(f"[{label}]({url})")
for w in dex.get("websites", []):
    url = w.get("url", "")
    if url: soc_parts.append(f"[Web]({url})")
if tg:  soc_parts.append(f"[TG]({tg})")
if web and not soc_parts: soc_parts.append(f"[Web]({web})")
soc_line = " · ".join(soc_parts) if soc_parts else "None"

th_line  = "|".join(str(x) for x in top_h) if top_h else "?"
s_emoji  = "🔥" if score >= 80 else "⚡" if score >= 60 else "👀"

return (
    f"{s_emoji} *{tag}*\n"
    f"Token: *{dex['name']}* (${dex['symbol']})\n"
    f"CA: `{addr}`\n"
    f"└ #{dex['chain'].upper()} | ⏱ {dex['pair_age']} | 🔗 [Chart]({dex['url']})\n"
    f"\n"
    f"📊 *Stats*\n"
    f"├ USD    `${dex['price']:.8f}` ({dex['price_1h']:+.1f}% 1h)\n"
    f"├ MC     `${fmt(dex['mc'])}`\n"
    f"├ Vol    `${fmt(dex['vol_24h'])}` · 5m: `${fmt(dex['vol_5m'])}`\n"
    f"├ LP     `${fmt(dex['lp'])}`\n"
    f"├ B/S    `{dex['buys_5m']}` / `{dex['sells_5m']}` (5m)\n"
    f"├ Migration  {'✅ YES' if migrated else f'❌ NO ({bc:.0f}% bonded)'}\n"
    f"├ Fees Paid  `{fees:.4f} SOL`\n"
    f"\n"
    f"🔒 *Security*\n"
    f"├ Top 10   `{top10}%` | `{hcount}` total holders\n"
    f"├ TH       `{th_line}`\n"
    f"├ Dev Sold  `{dev_h}%` holding\n"
    f"└ DEX Paid  {'✅' if dex['dex_paid'] else '❌'}\n"
    f"\n"
    f"🔗 *Socials*\n"
    f"└ {soc_line}\n"
    f"\n"
    f"{s_emoji} Score: *{score}/100*"
)
```

# ════════════════════════════════════════════════════════════════════════════

# FULL ANALYSE

# ════════════════════════════════════════════════════════════════════════════

async def analyse(address: str):
dex     = await get_dex_data(address)
if not dex:
return None, None, {}, 0.0, 0, []
pump    = await get_pump_data(address)
holders = await get_holders(address)
fees    = await get_fees_paid(address)
score, notes = score_token(dex, pump, holders)
return dex, pump, holders, fees, score, notes

# ════════════════════════════════════════════════════════════════════════════

# PNL CARD

# ════════════════════════════════════════════════════════════════════════════

def make_pnl_card(bg_bytes: bytes, name: str, symbol: str,
called_mc: float, current_mc: float, called_at_ts: float) -> io.BytesIO:
bg = Image.open(io.BytesIO(bg_bytes)).convert(“RGBA”)
bg = bg.resize((800, 450), Image.LANCZOS)

```
overlay = Image.new("RGBA", bg.size, (0, 0, 0, 160))
bg      = Image.alpha_composite(bg, overlay)
draw    = ImageDraw.Draw(bg)

mult      = (current_mc / called_mc) if called_mc else 1.0
mult_str  = f"{mult:.1f}x"
elapsed   = time.time() - called_at_ts
d, h      = int(elapsed // 86400), int((elapsed % 86400) // 3600)
since_val = f"{d}d, {h}h" if d else f"{h}h"

try:
    font_big  = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 90)
    font_mid  = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 38)
    font_sm   = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 24)
    font_name = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 52)
except Exception:
    font_big = font_mid = font_sm = font_name = ImageFont.load_default()

W, H   = bg.size
GREEN  = (0, 255, 100, 255)
WHITE  = (255, 255, 255, 255)
GREY   = (180, 180, 180, 255)
YELLOW = (255, 220, 0, 255)

draw.text((W - 20, 30),      f"${symbol}",                font=font_name, fill=WHITE,  anchor="ra")
draw.text((W - 20, 90),      name,                         font=font_sm,   fill=GREY,   anchor="ra")
draw.text((40, 30),          f"called at ${fmt(called_mc, 0)}", font=font_mid, fill=GREY)
draw.text((W // 2, H // 2 - 20), mult_str,                font=font_big,  fill=GREEN,  anchor="mm")
draw.text((W // 2, H // 2 + 65), f"⏱ {since_val}",       font=font_sm,   fill=WHITE,  anchor="mm")
draw.text((20, H - 30),      "🚀 GemStalker",              font=font_sm,   fill=YELLOW)

out = bg.convert("RGB")
buf = io.BytesIO()
out.save(buf, format="JPEG", quality=92)
buf.seek(0)
return buf
```

# ════════════════════════════════════════════════════════════════════════════

# COMMANDS

# ════════════════════════════════════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
await update.message.reply_text(
“🚀 *Alpha Scanner Bot*\n\n”
“📡 *Tracking:*\n”
“• New Pairs\n”
“• Pump.fun Migrations\n”
“• Smart Money Buys\n\n”
“🔽 *Filters:*\n”
“• MCAP: $5k–$50k\n”
“• LP > $4k\n”
“• Top 10 < 35%\n”
“• Dev Holding ≤ 1%\n”
“• Vol > $6k · Holders ≥ 20\n”
“• Requires Socials\n\n”
“📋 *Commands:*\n”
“`/calls` — all calls\n”
“`/calls 1h` · `/calls 6h` · `/calls 1d` · `/calls 7d`\n”
“`/scan <CA>` — deep scan + insights\n”
“`/pnl <CA>` — PNL card generator\n”
“`/status` — bot stats”,
parse_mode=“Markdown”,
)

async def scan_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
args = context.args
if not args:
await update.message.reply_text(“Usage: `/scan <CA>`”, parse_mode=“Markdown”)
return
ca  = args[0].strip()
msg = await update.message.reply_text(“🔍 Analysing…”)
dex, pump, holders, fees, score, notes = await analyse(ca)
if not dex:
await msg.edit_text(“❌ Token not found on DexScreener.”)
return
passed, reason = passes_filters(dex, pump, holders, fees)
alert   = build_alert(dex, pump, holders, fees, score,
tag=“✅ PASSES FILTERS” if passed else f”⚠️ FILTERED — {reason}”)
insight = “\n”.join(f”  {n}” for n in notes)
await msg.edit_text(
alert + f”\n\n💡 *Insights:*\n{insight}”,
parse_mode=“Markdown”,
disable_web_page_preview=True,
)

async def calls_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
windows = {“1h”: 3600, “6h”: 21600, “12h”: 43200, “1d”: 86400, “7d”: 604800}
args    = context.args
window  = None
label   = “all time”
if args and args[0].lower() in windows:
window = windows[args[0].lower()]
label  = args[0].lower()

```
now    = time.time()
subset = [c for c in call_history if window is None or (now - c["ts"]) <= window]

if not subset:
    await update.message.reply_text(f"No calls in {label}.")
    return

lines = [
    f"{i}. *{c['name']}* (${c['symbol']}) — MC `${fmt(c['mc'])}` | Score `{c['score']}/100` | _{since_str(c['ts'])} ago_"
    for i, c in enumerate(subset, 1)
]
await update.message.reply_text(
    f"📣 *Calls — {label}* ({len(subset)} total)\n\n" + "\n".join(lines),
    parse_mode="Markdown",
)
```

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
uptime = str(datetime.timedelta(seconds=int(time.time() - bot_start_time)))
await update.message.reply_text(
f”✅ *GemStalker Status*\n”
f”⏱ Uptime: `{uptime}`\n”
f”👁 Seen: `{len(seen_tokens)}` tokens\n”
f”📣 Calls: `{len(call_history)}`\n”
f”🔴 Stream: live (no 20s delay)”,
parse_mode=“Markdown”,
)

# ── /pnl flow ────────────────────────────────────────────────────────────────

async def pnl_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
args = context.args
if not args:
await update.message.reply_text(“Usage: `/pnl <CA>`”, parse_mode=“Markdown”)
return ConversationHandler.END
ca  = args[0].strip()
uid = update.effective_user.id
record = next((c for c in call_history if c[“address”] == ca), None)
if not record:
dex, _, _, _, score, _ = await analyse(ca)
if not dex:
await update.message.reply_text(“❌ Token not found.”)
return ConversationHandler.END
record = {“address”: ca, “name”: dex[“name”], “symbol”: dex[“symbol”],
“mc”: dex[“mc”], “price”: dex[“price”], “ts”: time.time(), “score”: score}
pnl_pending[uid] = {“ca”: ca, “record”: record}
await update.message.reply_text(
f”📸 Send me the background image you want for your *{record[‘name’]}* PNL card.”,
parse_mode=“Markdown”,
)
return WAIT_PHOTO

async def pnl_photo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
uid = update.effective_user.id
if uid not in pnl_pending:
return ConversationHandler.END
pending    = pnl_pending.pop(uid)
record     = pending[“record”]
photo_file = await update.message.photo[-1].get_file()
photo_bytes = await photo_file.download_as_bytearray()
dex, _, _, _, _, _ = await analyse(record[“address”])
current_mc = dex[“mc”] if dex else record[“mc”]
msg = await update.message.reply_text(“🎨 Generating card…”)
try:
buf = make_pnl_card(
bg_bytes=bytes(photo_bytes), name=record[“name”], symbol=record[“symbol”],
called_mc=record[“mc”], current_mc=current_mc, called_at_ts=record[“ts”],
)
await update.message.reply_photo(
photo=buf,
caption=f”🚀 *{record[‘name’]}* PNL Card\nCalled at `${fmt(record['mc'])}` MC”,
parse_mode=“Markdown”,
)
await msg.delete()
except Exception as e:
logger.error(f”pnl_photo error: {e}”)
await msg.edit_text(“❌ Failed to generate card. Try a different image.”)
return ConversationHandler.END

async def pnl_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
pnl_pending.pop(update.effective_user.id, None)
await update.message.reply_text(“Cancelled.”)
return ConversationHandler.END

async def msg_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
text = update.message.text.strip()
if re.match(r”^[1-9A-HJ-NP-Za-km-z]{32,44}$”, text) or re.match(r”^0x[0-9a-fA-F]{40}$”, text):
context.args = [text]
await scan_cmd(update, context)
else:
await update.message.reply_text(“Send a contract address or use /scan <CA>.”)

# ════════════════════════════════════════════════════════════════════════════

# REAL-TIME STREAM  (no 20s polling — fires instantly on discovery)

# ════════════════════════════════════════════════════════════════════════════

async def stream_loop(app):
logger.info(“Stream loop started — listening for new tokens”)
while True:
try:
addresses = set()

```
        profiles = await fetch(DEX_PROFILES)
        if isinstance(profiles, list):
            for p in profiles:
                a = p.get("tokenAddress") or p.get("address")
                if a: addresses.add(a)

        boosts = await fetch(DEX_BOOSTS)
        if isinstance(boosts, list):
            for b in boosts:
                a = b.get("tokenAddress") or b.get("address")
                if a: addresses.add(a)

        pump_list = await fetch(PUMP_COINS)
        if isinstance(pump_list, list):
            for coin in pump_list:
                a = coin.get("mint")
                if a: addresses.add(a)

        fresh = [a for a in addresses if a not in seen_tokens]

        for addr in fresh:
            seen_tokens[addr] = {"ts": time.time()}   # mark immediately

            dex, pump, holders, fees, score, notes = await analyse(addr)
            if not dex:
                continue

            passed, reason = passes_filters(dex, pump, holders, fees)
            if not passed:
                logger.debug(f"Filtered {addr}: {reason}")
                continue

            record = {
                "address": addr, "name": dex["name"], "symbol": dex["symbol"],
                "mc": dex["mc"], "price": dex["price"],
                "ts": time.time(), "score": score,
            }
            call_history.appendleft(record)
            seen_tokens[addr].update({
                "name": dex["name"], "symbol": dex["symbol"],
                "mc": dex["mc"], "price": dex["price"],
                "next_milestone": 2,
            })

            alert = build_alert(dex, pump, holders, fees, score)
            if CHAT_ID:
                await app.bot.send_message(
                    chat_id=CHAT_ID, text=alert,
                    parse_mode="Markdown", disable_web_page_preview=True,
                )
            logger.info(f"Alerted: {dex['name']} score={score}")

        await check_milestones(app)

    except Exception as e:
        logger.error(f"stream_loop error: {e}")

    await asyncio.sleep(3)   # courtesy pause — near real-time
```

async def check_milestones(app):
now = time.time()
for addr, info in list(seen_tokens.items()):
if “mc” not in info or not info.get(“next_milestone”):
continue
if now - info.get(“ts”, 0) > 172800:   # stop tracking after 48h
info[“next_milestone”] = None
continue

```
    dex = await get_dex_data(addr)
    if not dex:
        continue

    orig = info["mc"]
    curr = dex["mc"]
    if not orig:
        continue

    mult      = curr / orig
    milestone = info["next_milestone"]
    if mult >= milestone:
        next_m = next((m for m in PUMP_MILESTONES if m > milestone), None)
        info["next_milestone"] = next_m
        if CHAT_ID:
            try:
                await app.bot.send_message(
                    chat_id=CHAT_ID,
                    text=(
                        f"🚀 *PUMP UPDATE — {info['name']}*\n"
                        f"Called at `${fmt(orig)}` MC\n"
                        f"Now: `${fmt(curr)}` MC\n"
                        f"📈 *{mult:.1f}x* from call\n"
                        f"CA: `{addr}`"
                    ),
                    parse_mode="Markdown",
                )
            except Exception as e:
                logger.error(f"milestone alert error: {e}")
```

# ════════════════════════════════════════════════════════════════════════════

# MAIN

# ════════════════════════════════════════════════════════════════════════════

def main():
threading.Thread(target=run_flask, daemon=True).start()
logger.info(“Flask started”)

```
app = Application.builder().token(TOKEN).build()

pnl_conv = ConversationHandler(
    entry_points=[CommandHandler("pnl", pnl_cmd)],
    states={WAIT_PHOTO: [
        MessageHandler(filters.PHOTO, pnl_photo),
        CommandHandler("cancel", pnl_cancel),
    ]},
    fallbacks=[CommandHandler("cancel", pnl_cancel)],
)

app.add_handler(CommandHandler("start",  start))
app.add_handler(CommandHandler("scan",   scan_cmd))
app.add_handler(CommandHandler("status", status_cmd))
app.add_handler(CommandHandler("calls",  calls_cmd))
app.add_handler(pnl_conv)
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, msg_handler))

async def post_init(application):
    asyncio.create_task(stream_loop(application))

app.post_init = post_init

logger.info("GemStalker live")
app.run_polling(drop_pending_updates=True)
```

if __name__ == “__main__”:
main()