"""
GemStalker — Pre-Migration Bonding Curve Scanner
=================================================
Architecture:
  • pump.fun WebSocket  → real-time trade/mint events on bonding curve
  • Helius WebSocket    → on-chain logs, Raydium migration detection
  • pump.fun REST API   → enrich token metadata after first event
  • RugCheck API        → mint/freeze authority + bundle/sniper checks
  • DexScreener REST    → ONLY used post-migration for Raydium LP data
  • Solscan REST        → holder distribution

Alerts fire when a bonding-curve token passes ALL filters.
Milestones fire at 2x, 5x, 10x, 25x, 50x, 100x from call MC only.
"""

import os, re, io, time, json, asyncio, logging, datetime, threading
from collections   import deque
from dataclasses   import dataclass, field
from typing        import Optional

import httpx
import websockets
from PIL            import Image, ImageDraw, ImageFont
from flask          import Flask
from telegram       import Update
from telegram.ext   import (
    Application, CommandHandler, MessageHandler,
    ConversationHandler, ContextTypes, filters as tg_filters,
)

# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# ENV / KEYS
# ═══════════════════════════════════════════════════════════════════════════════

TG_TOKEN       = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID        = os.getenv("CHAT_ID", "")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")          # free at helius.dev

# ═══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

PUMP_WS    = "wss://pumpportal.fun/api/data"
HELIUS_WS  = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# ═══════════════════════════════════════════════════════════════════════════════
# REST ENDPOINTS  (enrichment only — not used for discovery)
# ═══════════════════════════════════════════════════════════════════════════════

PUMP_REST      = "https://frontend-api.pump.fun/coins/"
DEX_TOKEN      = "https://api.dexscreener.com/latest/dex/tokens/"
SOLSCAN_HOLD   = "https://public-api.solscan.io/token/holders?tokenAddress="
SOLSCAN_META   = "https://public-api.solscan.io/token/meta?tokenAddress="
RUGCHECK_BASE  = "https://api.rugcheck.xyz/v1"
SOL_PRICE_URL  = "https://price.jup.ag/v4/price?ids=SOL"

# Raydium v4 AMM program — subscribing to its logs catches pool creation = migration
RAYDIUM_AMM    = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"

# ═══════════════════════════════════════════════════════════════════════════════
# FILTER THRESHOLDS
# ═══════════════════════════════════════════════════════════════════════════════

F_MC_MIN         = 5_000       # USD
F_MC_MAX         = 50_000      # USD
F_SOL_IN_MIN     = 8           # SOL in bonding curve
F_HOLDERS_MIN    = 40
F_BUYS_PM_MIN    = 25          # buys per minute (2-min window)
F_TOP_HOLDER_MAX = 20          # % single wallet max
F_DEV_SOLD_MAX   = 1           # % dev may still hold
F_MAX_AGE_SECS   = 5 * 3600   # 5 h
F_REQUIRE_SOC    = True
F_MIG_PROB_MIN   = 60          # % heuristic

# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONES
# ═══════════════════════════════════════════════════════════════════════════════

MILESTONES = [2, 5, 10, 25, 50, 100]

# ═══════════════════════════════════════════════════════════════════════════════
# SHARED STATE
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TokenState:
    mint          : str
    name          : str    = "Unknown"
    symbol        : str    = "?"
    created_ts    : float  = 0.0
    dev_wallet    : str    = ""
    # live bonding curve stats
    sol_in        : float  = 0.0        # SOL entered bonding curve
    market_cap    : float  = 0.0        # USD
    holders       : int    = 0
    top_holder    : float  = 0.0        # % of supply
    top5          : list   = field(default_factory=list)
    dev_holding   : float  = 0.0        # % dev still holds
    bonding_pct   : float  = 0.0        # % bonding curve filled
    buy_times     : deque  = field(default_factory=lambda: deque(maxlen=300))
    # socials
    twitter       : str    = ""
    telegram      : str    = ""
    website       : str    = ""
    # security
    mint_revoked  : bool   = False
    freeze_revoked: bool   = False
    bundled       : bool   = False
    rugcheck_label: str    = "unknown"
    rugcheck_risks: list   = field(default_factory=list)
    # migration
    migrated      : bool   = False
    raydium_pool  : str    = ""
    lp_usd        : float  = 0.0
    # call tracking
    called        : bool   = False
    called_mc     : float  = 0.0
    called_ts     : float  = 0.0
    next_milestone: Optional[int] = 2

tokens       : dict[str, TokenState] = {}
call_history : deque                 = deque(maxlen=500)
bot_start    : float                 = time.time()
sol_price    : float                 = 150.0

http      = httpx.AsyncClient(timeout=12)
flask_app = Flask(__name__)
WAIT_PHOTO = 1

# ═══════════════════════════════════════════════════════════════════════════════
# FLASK HEALTH CHECK
# ═══════════════════════════════════════════════════════════════════════════════

@flask_app.route("/")
def health():
    return {"status": "alive", "tracked": len(tokens), "calls": len(call_history)}

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def clean(addr: str) -> str:
    """Strip pump.fun 'pump' suffix + whitespace from any address."""
    if not addr:
        return addr
    addr = addr.strip()
    if addr.endswith("pump"):
        addr = addr[:-4]
    return addr

def fmt(n, d: int = 2) -> str:
    if n is None: return "?"
    n = float(n)
    if n >= 1_000_000_000: return f"{n/1e9:.{d}f}B"
    if n >= 1_000_000:     return f"{n/1e6:.{d}f}M"
    if n >= 1_000:         return f"{n/1e3:.{d}f}K"
    return f"{n:.{d}f}"

def age_str(ts_secs: float) -> str:
    s = time.time() - ts_secs
    if s < 60:    return f"{int(s)}s"
    if s < 3600:  return f"{int(s/60)}m"
    if s < 86400: return f"{s/3600:.1f}h"
    return f"{s/86400:.1f}d"

def buys_per_minute(s: TokenState) -> float:
    cutoff = time.time() - 120
    return sum(1 for t in s.buy_times if t >= cutoff) / 2.0

def migration_probability(s: TokenState) -> int:
    score  = min(s.bonding_pct, 50)
    score += min(buys_per_minute(s) * 0.5, 20)
    if s.holders >= 100: score += 15
    elif s.holders >= 40: score += 8
    if s.sol_in >= 20: score += 15
    elif s.sol_in >= 8: score += 8
    return min(int(score), 100)

async def fetch(url: str, json_body: dict = None):
    try:
        r = await http.post(url, json=json_body) if json_body else await http.get(url)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"fetch error {url}: {e}")
        return None

# ═══════════════════════════════════════════════════════════════════════════════
# SOL PRICE — refreshed every 60 s
# ═══════════════════════════════════════════════════════════════════════════════

async def refresh_sol_price():
    global sol_price
    while True:
        try:
            data = await fetch(SOL_PRICE_URL)
            p = float((data or {}).get("data", {}).get("SOL", {}).get("price") or 0)
            if p > 0:
                sol_price = p
        except Exception:
            pass
        await asyncio.sleep(60)

# ═══════════════════════════════════════════════════════════════════════════════
# ENRICHMENT SOURCES
# ═══════════════════════════════════════════════════════════════════════════════

async def enrich_from_pump(mint: str) -> dict:
    data = await fetch(PUMP_REST + mint)
    if not data:
        return {}
    real_sol = float(data.get("real_sol_reserves") or 0) / 1e9
    virt_sol = float(data.get("virtual_sol_reserves") or 0) / 1e9
    return {
        "name"        : data.get("name", "Unknown"),
        "symbol"      : data.get("symbol", "?"),
        "dev_wallet"  : data.get("creator", ""),
        "dev_holding" : float(data.get("creator_percentage") or 0),
        "bonding_pct" : float(data.get("bonding_curve_percentage") or 0),
        "sol_in"      : real_sol if real_sol > 0 else virt_sol,
        "market_cap"  : float(data.get("usd_market_cap") or 0),
        "migrated"    : data.get("raydium_pool") is not None,
        "raydium_pool": data.get("raydium_pool") or "",
        "twitter"     : data.get("twitter") or "",
        "telegram"    : data.get("telegram") or "",
        "website"     : data.get("website") or "",
        "created_ts"  : float(data.get("created_timestamp") or 0) / 1000,
    }

async def enrich_holders(mint: str) -> dict:
    result = {"holders": 0, "top_holder": 0.0, "top5": []}
    data   = await fetch(f"{SOLSCAN_HOLD}{mint}&limit=10&offset=0")
    if not data:
        return result
    hlist  = data.get("data", [])
    meta   = await fetch(f"{SOLSCAN_META}{mint}")
    supply = float((meta or {}).get("supply") or 0)
    if supply and hlist:
        pcts = [round(float(h.get("amount") or 0) / supply * 100, 2) for h in hlist[:5]]
        result["top_holder"] = pcts[0] if pcts else 0.0
        result["top5"]       = pcts
    result["holders"] = data.get("total") or 0
    return result

async def enrich_rugcheck(mint: str) -> dict:
    result = {
        "mint_revoked"  : False,
        "freeze_revoked": False,
        "bundled"       : False,
        "score_label"   : "unknown",
        "risks"         : [],
    }
    data = await fetch(f"{RUGCHECK_BASE}/tokens/{mint}/report/summary")
    if not data:
        return result
    score  = data.get("score") or 0
    risks  = [r.get("name", "") for r in (data.get("risks") or [])]
    result.update({
        "mint_revoked"  : data.get("mintDisabled", False),
        "freeze_revoked": data.get("freezeDisabled", False),
        "bundled"       : any("bundle" in r.lower() for r in risks),
        "risks"         : risks,
        "score_label"   : ("✅ Good" if score < 300 else "⚠️ Risky" if score < 700 else "❌ Danger"),
    })
    return result

async def enrich_dex(mint: str) -> dict:
    """Post-migration only — fetch Raydium LP from DexScreener."""
    data = await fetch(DEX_TOKEN + mint)
    if not data:
        return {}
    pairs = data.get("pairs") or []
    if not pairs:
        return {}
    p = pairs[0]
    return {
        "lp_usd"   : float((p.get("liquidity") or {}).get("usd") or 0),
        "mc_dex"   : float(p.get("fdv") or 0),
        "dex_url"  : p.get("url", ""),
    }

async def full_enrich(s: TokenState):
    """Pull from all sources and update state in-place."""
    pump = await enrich_from_pump(s.mint)
    if pump:
        s.name         = pump.get("name") or s.name
        s.symbol       = pump.get("symbol") or s.symbol
        s.dev_wallet   = pump.get("dev_wallet") or s.dev_wallet
        s.dev_holding  = pump.get("dev_holding", s.dev_holding)
        s.bonding_pct  = pump.get("bonding_pct", s.bonding_pct)
        s.sol_in       = pump.get("sol_in") or s.sol_in
        s.market_cap   = pump.get("market_cap") or s.market_cap
        s.migrated     = pump.get("migrated", s.migrated)
        s.raydium_pool = pump.get("raydium_pool") or s.raydium_pool
        s.twitter      = pump.get("twitter") or s.twitter
        s.telegram     = pump.get("telegram") or s.telegram
        s.website      = pump.get("website") or s.website
        if pump.get("created_ts", 0) > 0 and s.created_ts == 0:
            s.created_ts = pump["created_ts"]

    hold = await enrich_holders(s.mint)
    s.holders    = hold.get("holders", s.holders)
    s.top_holder = hold.get("top_holder", s.top_holder)
    s.top5       = hold.get("top5", s.top5)

    rug = await enrich_rugcheck(s.mint)
    s.mint_revoked    = rug.get("mint_revoked", s.mint_revoked)
    s.freeze_revoked  = rug.get("freeze_revoked", s.freeze_revoked)
    s.bundled         = rug.get("bundled", s.bundled)
    s.rugcheck_label  = rug.get("score_label", s.rugcheck_label)
    s.rugcheck_risks  = rug.get("risks", s.rugcheck_risks)

    if s.migrated:
        dex = await enrich_dex(s.mint)
        if dex.get("lp_usd", 0) > 0:
            s.lp_usd = dex["lp_usd"]
        if dex.get("mc_dex", 0) > 0:
            s.market_cap = dex["mc_dex"]

# ═══════════════════════════════════════════════════════════════════════════════
# FILTER GATE
# ═══════════════════════════════════════════════════════════════════════════════

def passes_filters(s: TokenState) -> tuple[bool, str]:
    if s.created_ts > 0 and (time.time() - s.created_ts) > F_MAX_AGE_SECS:
        return False, f"Too old — {age_str(s.created_ts)} (max 5h)"

    if not (F_MC_MIN <= s.market_cap <= F_MC_MAX):
        return False, f"MC ${fmt(s.market_cap)} outside $5k–$50k"

    # Liquidity: pre-migration = SOL in bonding curve; post = Raydium LP
    lp_usd = s.lp_usd if s.migrated else s.sol_in * sol_price
    if lp_usd < F_SOL_IN_MIN * sol_price:
        return False, f"Low liquidity — {s.sol_in:.2f} SOL in curve (min {F_SOL_IN_MIN} SOL)"

    if s.holders < F_HOLDERS_MIN:
        return False, f"Only {s.holders} holders (min {F_HOLDERS_MIN})"

    bpm = buys_per_minute(s)
    if bpm < F_BUYS_PM_MIN:
        return False, f"{bpm:.1f} buys/min (min {F_BUYS_PM_MIN})"

    if s.top_holder > F_TOP_HOLDER_MAX:
        return False, f"Top holder {s.top_holder:.1f}% > {F_TOP_HOLDER_MAX}%"

    if s.dev_holding > F_DEV_SOLD_MAX:
        return False, f"Dev holding {s.dev_holding:.1f}% > {F_DEV_SOLD_MAX}%"

    if s.bundled:
        return False, "Bundled wallets detected"

    if F_REQUIRE_SOC and not any([s.twitter, s.telegram, s.website]):
        return False, "No socials"

    mp = migration_probability(s)
    if mp < F_MIG_PROB_MIN:
        return False, f"Migration probability {mp}% < {F_MIG_PROB_MIN}%"

    return True, "ok"

# ═══════════════════════════════════════════════════════════════════════════════
# ALERT BUILDER
# ═══════════════════════════════════════════════════════════════════════════════

def build_alert(s: TokenState, tag: str = "🔥 GEM FOUND") -> str:
    bpm    = buys_per_minute(s)
    mp     = migration_probability(s)
    lp_str = (f"${fmt(s.lp_usd)} (Raydium)" if s.migrated
              else f"{s.sol_in:.2f} SOL in bonding curve")

    soc = []
    if s.twitter:  soc.append(f"[X]({s.twitter})")
    if s.telegram: soc.append(f"[TG]({s.telegram})")
    if s.website:  soc.append(f"[Web]({s.website})")
    soc_line = " · ".join(soc) if soc else "None"

    top5_str = " | ".join(f"{p}%" for p in s.top5) if s.top5 else "?"

    ca      = s.mint
    photon  = f"https://photon-sol.tinyastro.io/en/r/@{ca}"
    bullx   = f"https://bullx.io/terminal?chainId=1399811149&address={ca}"
    trojan  = f"https://t.me/solana_trojanbot?start={ca}"
    dex_url = f"https://dexscreener.com/solana/{ca}"

    emoji = "🔥" if mp >= 80 else "⚡" if mp >= 60 else "👀"

    return (
        f"{emoji} *{tag}*\n"
        f"Token: *{s.name}* (${s.symbol})\n"
        f"CA: `{ca}`\n"
        f"Age: {age_str(s.created_ts)}\n"
        f"\n"
        f"📊 *Bonding Curve*\n"
        f"├ MC           `${fmt(s.market_cap)}`\n"
        f"├ Liquidity    `{lp_str}`\n"
        f"├ Bonding      `{s.bonding_pct:.1f}%` filled\n"
        f"├ Buys/min     `{bpm:.1f}`\n"
        f"├ Holders      `{s.holders}`\n"
        f"├ Mig. Prob.   `{mp}%`\n"
        f"└ Migration    {'✅ YES — on Raydium' if s.migrated else '⏳ NOT YET'}\n"
        f"\n"
        f"🔒 *Security*\n"
        f"├ RugCheck     `{s.rugcheck_label}`\n"
        f"├ Top holder   `{s.top_holder:.1f}%`\n"
        f"├ Top 5        `{top5_str}`\n"
        f"├ Dev holding  `{s.dev_holding:.1f}%`\n"
        f"├ Mint revoked `{'✅' if s.mint_revoked else '❌'}`\n"
        f"├ Freeze rev.  `{'✅' if s.freeze_revoked else '❌'}`\n"
        f"└ Bundled      `{'❌ YES — avoid' if s.bundled else '✅ Clean'}`\n"
        f"\n"
        f"🔗 *Socials*\n"
        f"└ {soc_line}\n"
        f"\n"
        f"⚡ *Trade*\n"
        f"[Photon]({photon}) · [BullX]({bullx}) · [Trojan]({trojan}) · [DEX]({dex_url})\n"
        f"\n"
        f"{emoji} Mig. Probability: *{mp}%*"
    )

# ═══════════════════════════════════════════════════════════════════════════════
# ALERT SENDER + CALL RECORDER
# ═══════════════════════════════════════════════════════════════════════════════

async def send_alert(app, s: TokenState, tag: str = "🔥 GEM FOUND"):
    if not CHAT_ID:
        return
    try:
        await app.bot.send_message(
            chat_id=CHAT_ID, text=build_alert(s, tag),
            parse_mode="Markdown", disable_web_page_preview=True,
        )
    except Exception as e:
        log.error(f"send_alert: {e}")

def record_call(s: TokenState):
    call_history.appendleft({
        "mint": s.mint, "name": s.name, "symbol": s.symbol,
        "mc": s.market_cap, "ts": time.time(),
    })
    s.called = True
    s.called_mc = s.market_cap
    s.called_ts = time.time()
    s.next_milestone = 2

# ═══════════════════════════════════════════════════════════════════════════════
# ENRICH + FILTER + ALERT
# ═══════════════════════════════════════════════════════════════════════════════

async def enrich_and_maybe_alert(app, mint: str, force: bool = False):
    s = tokens.get(mint)
    if not s or (s.called and not force):
        return

    await full_enrich(s)

    if s.created_ts > 0 and (time.time() - s.created_ts) > F_MAX_AGE_SECS:
        return

    passed, reason = passes_filters(s)
    if not passed:
        log.debug(f"Filtered {s.name} ({mint}): {reason}")
        return

    log.info(f"✅ CALLING {s.name} (${s.symbol}) MC=${fmt(s.market_cap)} bpm={buys_per_minute(s):.1f}")
    record_call(s)
    tag = "🚀 MIGRATED GEM" if s.migrated else "🔥 PRE-MIGRATION GEM"
    await send_alert(app, s, tag)

# ═══════════════════════════════════════════════════════════════════════════════
# PUMP.FUN WEBSOCKET
# Docs: https://pumpportal.fun/
# ═══════════════════════════════════════════════════════════════════════════════

async def pump_ws_loop(app):
    backoff = 2
    while True:
        try:
            log.info("Connecting to pump.fun WebSocket…")
            async with websockets.connect(
                PUMP_WS, ping_interval=20, ping_timeout=30, close_timeout=10,
            ) as ws:
                backoff = 2
                # Subscribe to new token mints AND all trades
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeTokenTrade"}))
                log.info("pump.fun WS connected and subscribed ✓")
                async for raw in ws:
                    try:
                        await handle_pump_event(app, json.loads(raw))
                    except Exception as e:
                        log.debug(f"pump WS event error: {e}")
        except Exception as e:
            log.warning(f"pump.fun WS dropped: {e} — retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def handle_pump_event(app, msg: dict):
    txtype = msg.get("txType") or msg.get("type") or ""
    mint   = clean(msg.get("mint") or msg.get("tokenAddress") or "")
    if not mint:
        return

    now = time.time()

    # ── New token minted ──────────────────────────────────────────────────────
    if txtype in ("create", "newCoin"):
        if mint in tokens:
            return
        s = TokenState(
            mint       = mint,
            created_ts = now,
            name       = msg.get("name", "Unknown"),
            symbol     = msg.get("symbol", "?"),
            dev_wallet = msg.get("creator", ""),
            sol_in     = float(msg.get("solAmount", 0)) / 1e9,
            market_cap = float(msg.get("marketCapSol", 0)) * sol_price,
        )
        tokens[mint] = s
        log.info(f"New token: {s.name} ({s.symbol}) {mint}")
        asyncio.create_task(enrich_and_maybe_alert(app, mint))
        return

    # ── Buy / Sell trade ──────────────────────────────────────────────────────
    if txtype in ("buy", "sell"):
        if mint not in tokens:
            s = TokenState(mint=mint, created_ts=now)
            tokens[mint] = s
            asyncio.create_task(enrich_and_maybe_alert(app, mint))

        s = tokens[mint]
        sol_amount = float(msg.get("solAmount", 0)) / 1e9
        new_mc     = float(msg.get("marketCapSol", 0)) * sol_price

        if txtype == "buy":
            s.sol_in += sol_amount
            s.buy_times.append(now)

        if new_mc > 0:
            s.market_cap = new_mc

        # Check milestones live from WS events (no REST call needed)
        if s.called and s.called_mc > 0:
            asyncio.create_task(check_milestone(app, mint))

        # Re-evaluate filter every 10 buys before first alert
        if not s.called and txtype == "buy" and len(s.buy_times) % 10 == 0:
            asyncio.create_task(enrich_and_maybe_alert(app, mint))
        return

    # ── Migration ─────────────────────────────────────────────────────────────
    if txtype in ("migrate", "migration") or msg.get("raydiumPool"):
        if mint not in tokens:
            tokens[mint] = TokenState(mint=mint, created_ts=now)
        s = tokens[mint]
        s.migrated     = True
        s.raydium_pool = msg.get("raydiumPool", "")
        log.info(f"Migration: {s.name} ({mint})")
        asyncio.create_task(enrich_and_maybe_alert(app, mint, force=True))

# ═══════════════════════════════════════════════════════════════════════════════
# HELIUS WEBSOCKET — Raydium AMM log subscription
# Catches migration events even if pump.fun WS misses them
# ═══════════════════════════════════════════════════════════════════════════════

async def helius_ws_loop(app):
    if not HELIUS_API_KEY:
        log.warning("No HELIUS_API_KEY set — Helius WS disabled. Migration detection via pump.fun WS only.")
        return

    backoff = 2
    while True:
        try:
            log.info("Connecting to Helius WebSocket…")
            async with websockets.connect(
                HELIUS_WS, ping_interval=20, ping_timeout=30,
            ) as ws:
                backoff = 2
                await ws.send(json.dumps({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "logsSubscribe",
                    "params": [
                        {"mentions": [RAYDIUM_AMM]},
                        {"commitment": "confirmed"},
                    ],
                }))
                log.info("Helius WS subscribed to Raydium AMM logs ✓")
                async for raw in ws:
                    try:
                        await handle_helius_event(app, json.loads(raw))
                    except Exception as e:
                        log.debug(f"Helius event error: {e}")
        except Exception as e:
            log.warning(f"Helius WS dropped: {e} — retry in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def handle_helius_event(app, msg: dict):
    result = msg.get("params", {}).get("result", {})
    if not result:
        return
    value  = result.get("value", {})
    if value.get("err"):
        return
    logs   = value.get("logs", [])
    # Only care about new pool initialization (migration = new Raydium pool)
    if not any("initialize" in l.lower() for l in logs):
        return
    log_str = " ".join(logs)
    for mint in list(tokens.keys()):
        if mint in log_str and not tokens[mint].migrated:
            log.info(f"Helius: migration detected for {tokens[mint].name} ({mint})")
            tokens[mint].migrated = True
            asyncio.create_task(enrich_and_maybe_alert(app, mint, force=True))
            break

# ═══════════════════════════════════════════════════════════════════════════════
# MILESTONE TRACKER
# ═══════════════════════════════════════════════════════════════════════════════

async def check_milestone(app, mint: str):
    s = tokens.get(mint)
    if not s or not s.called or not s.called_mc or not s.next_milestone:
        return
    if time.time() - s.called_ts > 172800:
        s.next_milestone = None
        return
    mult = s.market_cap / s.called_mc
    ms   = s.next_milestone
    if mult >= ms:
        s.next_milestone = next((m for m in MILESTONES if m > ms), None)
        if CHAT_ID:
            try:
                await app.bot.send_message(
                    chat_id=CHAT_ID,
                    text=(
                        f"🚀 *{ms}x MILESTONE — {s.name}*\n"
                        f"Called at `${fmt(s.called_mc)}` MC\n"
                        f"Now: `${fmt(s.market_cap)}` MC\n"
                        f"📈 *{mult:.1f}x* from call\n"
                        f"CA: `{mint}`"
                    ),
                    parse_mode="Markdown",
                )
            except Exception as e:
                log.error(f"milestone send: {e}")

async def milestone_poll_loop(app):
    """Polls pump.fun every 30 s for all called tokens to catch milestones."""
    while True:
        await asyncio.sleep(30)
        for mint, s in list(tokens.items()):
            if not s.called or not s.next_milestone:
                continue
            try:
                pump = await enrich_from_pump(mint)
                mc   = pump.get("market_cap", 0)
                if mc > 0:
                    s.market_cap = mc
                await check_milestone(app, mint)
            except Exception as e:
                log.debug(f"milestone poll {mint}: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# CLEANUP
# ═══════════════════════════════════════════════════════════════════════════════

async def cleanup_loop():
    while True:
        await asyncio.sleep(3600)
        cutoff = time.time() - 172800
        stale  = [m for m, s in tokens.items() if s.created_ts > 0 and s.created_ts < cutoff and not s.next_milestone]
        for m in stale:
            tokens.pop(m, None)
        if stale:
            log.info(f"Cleaned {len(stale)} stale tokens")

# ═══════════════════════════════════════════════════════════════════════════════
# PNL CARD
# ═══════════════════════════════════════════════════════════════════════════════

def make_pnl_card(bg_bytes: bytes, name: str, symbol: str,
                  called_mc: float, current_mc: float, called_at_ts: float) -> io.BytesIO:
    bg = Image.open(io.BytesIO(bg_bytes)).convert("RGBA")
    bg = bg.resize((800, 450), Image.LANCZOS)
    overlay = Image.new("RGBA", bg.size, (0, 0, 0, 160))
    bg      = Image.alpha_composite(bg, overlay)
    draw    = ImageDraw.Draw(bg)

    mult      = (current_mc / called_mc) if called_mc and called_mc > 0 else 1.0
    d, h      = divmod(int(time.time() - called_at_ts), 86400)
    h         = h // 3600
    since_val = f"{d}d, {h}h" if d else f"{h}h"

    font_big = font_mid = font_sm = font_name = ImageFont.load_default()
    for fp in ["/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
               "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
               "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf"]:
        if os.path.exists(fp):
            try:
                font_big  = ImageFont.truetype(fp, 90)
                font_mid  = ImageFont.truetype(fp, 38)
                font_sm   = ImageFont.truetype(fp, 24)
                font_name = ImageFont.truetype(fp, 52)
            except Exception:
                pass
            break

    W, H = bg.size
    draw.text((W-20, 30),        f"${symbol}",                     font=font_name, fill=(255,255,255,255), anchor="ra")
    draw.text((W-20, 90),        name,                              font=font_sm,   fill=(180,180,180,255), anchor="ra")
    draw.text((40, 30),          f"called at ${fmt(called_mc, 0)}", font=font_mid,  fill=(180,180,180,255))
    draw.text((W//2, H//2-20),   f"{mult:.1f}x",                   font=font_big,
              fill=((0,255,100,255) if mult >= 1 else (255,80,80,255)),             anchor="mm")
    draw.text((W//2, H//2+65),   f"since call: {since_val}",       font=font_sm,   fill=(255,255,255,255), anchor="mm")
    draw.text((40, H-50),        f"Called MC:  ${fmt(called_mc)}", font=font_sm,   fill=(180,180,180,255))
    draw.text((40, H-25),        f"Current MC: ${fmt(current_mc)}",font=font_sm,   fill=(255,255,255,255))
    draw.text((W-20, H-20),      "GemStalker",                      font=font_sm,   fill=(255,220,0,255),   anchor="ra")

    buf = io.BytesIO()
    bg.convert("RGB").save(buf, format="JPEG", quality=92)
    buf.seek(0)
    return buf

# ═══════════════════════════════════════════════════════════════════════════════
# TELEGRAM COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🚀 *GemStalker — Pre-Migration Scanner*\n\n"
        "📡 *Sources (real-time):*\n"
        "• pump.fun WebSocket — bonding curve events\n"
        "• Helius WebSocket — Raydium migration detection\n"
        "• RugCheck API — security checks\n\n"
        "🔽 *Filters:*\n"
        "• MC $5k–$50k\n"
        "• Liquidity ≥ 8 SOL in bonding curve\n"
        "• Holders ≥ 40\n"
        "• Buys/min ≥ 25\n"
        "• Top holder < 20%\n"
        "• Dev holding ≤ 1%\n"
        "• No bundled wallets\n"
        "• Migration probability ≥ 60%\n"
        "• Age ≤ 5h · Requires socials\n\n"
        "📋 *Commands:*\n"
        "`/scan <CA>` — deep scan any token\n"
        "`/calls` — this month's calls with X multiple\n"
        "`/pnl <CA>` — PNL card generator\n"
        "`/status` — bot stats",
        parse_mode="Markdown",
    )

async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Usage: `/scan <CA>`", parse_mode="Markdown")
        return
    mint = clean(ctx.args[0].strip())
    msg  = await update.message.reply_text("🔍 Scanning…")

    s = tokens.get(mint)
    if not s:
        s = TokenState(mint=mint)
        tokens[mint] = s
    await full_enrich(s)

    if s.name == "Unknown" and s.market_cap == 0:
        await msg.edit_text("❌ Token not found. Check the CA.")
        return

    passed, reason = passes_filters(s)
    tag    = "✅ PASSES FILTERS" if passed else f"⚠️ FILTERED — {reason}"
    bpm    = buys_per_minute(s)
    mp     = migration_probability(s)
    extra  = (
        f"\n\n💡 *Scan Details:*\n"
        f"  Buys/min: `{bpm:.1f}`\n"
        f"  Migration probability: `{mp}%`\n"
        f"  SOL in curve: `{s.sol_in:.2f}`\n"
        f"  Bonding filled: `{s.bonding_pct:.1f}%`\n"
        f"  Mint revoked: `{'✅' if s.mint_revoked else '❌'}`\n"
        f"  Freeze revoked: `{'✅' if s.freeze_revoked else '❌'}`\n"
        f"  RugCheck risks: `{', '.join(s.rugcheck_risks) or 'none'}`"
    )
    await msg.edit_text(
        build_alert(s, tag) + extra,
        parse_mode="Markdown", disable_web_page_preview=True,
    )

async def cmd_calls(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not call_history:
        await update.message.reply_text("No calls yet this session.")
        return
    month_ago  = time.time() - 30 * 86400
    this_month = [c for c in call_history if c["ts"] >= month_ago]
    if not this_month:
        await update.message.reply_text("No calls in the last 30 days.")
        return

    msg = await update.message.reply_text("📊 Fetching current prices…")
    lines, winners = [], 0
    for i, c in enumerate(this_month, 1):
        try:
            pump    = await enrich_from_pump(c["mint"])
            curr_mc = pump.get("market_cap") or c["mc"]
        except Exception:
            curr_mc = c["mc"]
        mult = curr_mc / c["mc"] if c["mc"] > 0 else 1.0
        if mult >= 2: winners += 1
        emoji = "🚀" if mult >= 2 else "📈" if mult >= 1.2 else "😐" if mult >= 0.8 else "📉"
        lines.append(
            f"{i}. {emoji} *{c['name']}* (${c['symbol']})\n"
            f"   `${fmt(c['mc'])}` → `${fmt(curr_mc)}` | *{mult:.1f}x*"
        )
    await msg.edit_text(
        f"📣 *Calls this month* — {len(this_month)} total | {winners} hit 2x+\n\n"
        + "\n\n".join(lines),
        parse_mode="Markdown",
    )

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uptime = str(datetime.timedelta(seconds=int(time.time() - bot_start)))
    await update.message.reply_text(
        f"✅ *GemStalker Status*\n"
        f"⏱ Uptime:     `{uptime}`\n"
        f"👁 Tracking:   `{len(tokens)}` tokens\n"
        f"📣 Calls:      `{len(call_history)}`\n"
        f"💰 SOL price:  `${sol_price:.2f}`\n"
        f"📡 Streams:    pump.fun WS + Helius WS",
        parse_mode="Markdown",
    )

# ── /pnl conversation ──────────────────────────────────────────────────────────

async def cmd_pnl(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    if not ctx.args:
        await update.message.reply_text("Usage: `/pnl <CA>`", parse_mode="Markdown")
        return ConversationHandler.END

    mint   = clean(ctx.args[0].strip())
    record = next((c for c in call_history if c["mint"] == mint), None)

    if not record:
        wait = await update.message.reply_text("🔍 Fetching token data…")
        s    = tokens.get(mint) or TokenState(mint=mint)
        await full_enrich(s)
        try:
            await wait.delete()
        except Exception:
            pass
        if s.market_cap == 0:
            await update.message.reply_text("❌ Token not found. Check the CA.")
            return ConversationHandler.END
        record = {"mint": mint, "name": s.name, "symbol": s.symbol, "mc": s.market_cap, "ts": time.time()}

    ctx.user_data["pnl_record"] = record
    await update.message.reply_text(
        f"📸 Send background image for your *{record['name']}* PNL card.\nSend /cancel to abort.",
        parse_mode="Markdown",
    )
    return WAIT_PHOTO

async def pnl_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    record = ctx.user_data.get("pnl_record")
    if not record:
        await update.message.reply_text("Session expired. Run `/pnl <CA>` again.", parse_mode="Markdown")
        return ConversationHandler.END
    try:
        f     = await update.message.photo[-1].get_file()
        data  = await f.download_as_bytearray()
    except Exception as e:
        log.error(f"pnl photo dl: {e}")
        await update.message.reply_text("❌ Could not download image. Try again.")
        return ConversationHandler.END

    try:
        pump    = await enrich_from_pump(record["mint"])
        curr_mc = pump.get("market_cap") or record["mc"]
    except Exception:
        curr_mc = record["mc"]

    msg = await update.message.reply_text("🎨 Generating PNL card…")
    try:
        buf  = make_pnl_card(bytes(data), record["name"], record["symbol"],
                             record["mc"], curr_mc, record["ts"])
        mult = curr_mc / record["mc"] if record["mc"] > 0 else 1.0
        await update.message.reply_photo(
            photo      = buf,
            caption    = (
                f"🚀 *{record['name']}* (${record['symbol']})\n"
                f"Called: `${fmt(record['mc'])}` → Now: `${fmt(curr_mc)}`\n"
                f"Performance: *{mult:.1f}x*"
            ),
            parse_mode = "Markdown",
        )
        await msg.delete()
    except Exception as e:
        log.error(f"pnl card gen: {e}")
        await msg.edit_text("❌ Failed to generate card. Try a JPEG or PNG image.")
    finally:
        ctx.user_data.pop("pnl_record", None)
    return ConversationHandler.END

async def pnl_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data.pop("pnl_record", None)
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END

async def msg_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    text = clean(update.message.text.strip())
    if re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", text) or re.match(r"^0x[0-9a-fA-F]{40}$", text):
        ctx.args = [text]
        await cmd_scan(update, ctx)
    else:
        await update.message.reply_text("Send a contract address or use /scan <CA>.")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    threading.Thread(target=run_flask, daemon=True).start()
    log.info("Flask health server started")

    app = Application.builder().token(TG_TOKEN).build()

    pnl_conv = ConversationHandler(
        entry_points  = [CommandHandler("pnl", cmd_pnl)],
        states        = {WAIT_PHOTO: [
            MessageHandler(tg_filters.PHOTO, pnl_photo),
            CommandHandler("cancel", pnl_cancel),
        ]},
        fallbacks     = [CommandHandler("cancel", pnl_cancel)],
        allow_reentry = True,
        name          = "pnl_conv",
    )

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("scan",   cmd_scan))
    app.add_handler(CommandHandler("calls",  cmd_calls))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(pnl_conv)
    app.add_handler(MessageHandler(tg_filters.TEXT & ~tg_filters.COMMAND, msg_handler))

    async def post_init(application):
        asyncio.create_task(refresh_sol_price())
        asyncio.create_task(pump_ws_loop(application))
        asyncio.create_task(helius_ws_loop(application))
        asyncio.create_task(milestone_poll_loop(application))
        asyncio.create_task(cleanup_loop())
        log.info("All streams running ✓")

    app.post_init = post_init
    log.info("GemStalker starting — pre-migration mode")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
