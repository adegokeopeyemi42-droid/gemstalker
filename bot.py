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
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID = os.getenv('CHAT_ID')
PUMPPORTAL_KEY = os.getenv('PUMPPORTAL_API_KEY', '')   # Optional but recommended
PUMP_WS = 'wss://pumpportal.fun/api/data'
SOL_PRICE = 150.0
SOLANA_CA_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')

# =========================================================
# FILTERS (pre-migration focused)
# =========================================================
MC_MIN          = 5_000
MC_MAX          = 300_000
MIN_POOL_SOL    = 10.0      # replaces MIN_NET_SOL — measures bonding curve depth (real liquidity)
MIN_PRESSURE    = 55
MIN_BPM         = 2.0
MIN_BUYS        = 3
MAX_AGE_MINUTES = 20
BC_MIN          = 15.0      # bonding curve % lower bound
BC_MAX          = 70.0      # bonding curve % upper bound
MIN_SCORE       = 40
ALERT_DELAY_SEC = 3
MILESTONES      = [2, 5, 10, 25, 50, 100]

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger(__name__)

# =========================================================
# DATA MODEL
# =========================================================
@dataclass
class Token:
    mint: str
    name: str          = 'Unknown'
    symbol: str        = '?'
    market_cap: float  = 0.0
    virtual_sol: float = 0.0   # bonding curve SOL reserves (real pool depth) — already in SOL
    sol_in: float      = 0.0   # cumulative buy volume in SOL  (display/volume only)
    sol_out: float     = 0.0   # cumulative sell volume in SOL (display/volume only)
    bonding_curve: float = 0.0
    buy_count: int     = 0
    sell_count: int    = 0
    migrated: bool     = False
    raydium_liq: float = 0.0
    called: bool       = False
    twitter: str       = ''
    telegram_link: str = ''
    website: str       = ''
    created_at: float  = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    buys_ts: deque     = field(default_factory=lambda: deque(maxlen=300))
    entry_mc: float    = 0.0
    next_milestone: int = 2
    last_score: int    = 0
    watchlist: bool    = False
    watchlist_since: float = 0.0

tokens: dict           = {}
recent_calls: deque    = deque(maxlen=500)
ev_count               = 0
buy_count_global       = 0
ws_event_counts: dict  = {}
subscribed_tokens: set = set()
MAX_SUBSCRIPTIONS      = 500

# =========================================================
# LIQUIDITY HELPERS
# ---------------------------------------------------------
# Pump.fun bonding curve seeds with ~30 SOL of virtual
# reserves. virtualSolReserves (scaled to SOL in the event
# handler) IS the pool depth — what Trojan / GMGN / Axiom
# all display as "Liquidity".
#
# sol_in / sol_out are VOLUME accumulators, NOT liquidity.
# They are kept only for the volume display line in alerts.
#
# Pre-migration  → pool_sol()  (bonding curve depth)
# Post-migration → raydium_liq (fetched from DexScreener)
# =========================================================
def pool_sol(t: Token) -> float:
    """SOL locked in the bonding curve — the real pool depth."""
    return t.virtual_sol

def effective_liq(t: Token) -> float:
    """Pool depth in USD (what trading UIs show as Liquidity)."""
    return pool_sol(t) * SOL_PRICE

def get_liq(t: Token) -> float:
    if t.migrated and t.raydium_liq > 0:
        return t.raydium_liq
    return effective_liq(t)

def liq_source(t: Token) -> str:
    if t.migrated and t.raydium_liq > 0:
        return 'Raydium'
    return 'Bonding Curve'

# =========================================================
# SOL PRICE UPDATE
# =========================================================
async def update_sol_price() -> None:
    global SOL_PRICE
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd'
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(url)
                SOL_PRICE = float(r.json()['solana']['usd'])
                log.info(f'SOL price updated: ${SOL_PRICE:.2f}')
        except Exception as e:
            log.debug(f'SOL price fetch failed: {e}')
        await asyncio.sleep(60)

# =========================================================
# HELPERS
# =========================================================
def fmt(n, dec=1) -> str:
    if n is None: return 'N/A'
    n = float(n)
    if n >= 1_000_000: return f'${n/1_000_000:.{dec}f}M'
    if n >= 1_000:     return f'${n/1_000:.{dec}f}K'
    return f'${n:.{dec}f}'

def bpm(t: Token) -> float:
    cutoff = time.time() - 60
    return float(sum(1 for x in t.buys_ts if x >= cutoff))

def pressure(t: Token) -> int:
    total = t.buy_count + t.sell_count
    return int(t.buy_count / total * 100) if total else 0

def age_str(t: Token) -> str:
    s = int(time.time() - t.created_at)
    if s < 60:   return f'{s}s'
    if s < 3600: return f'{s//60}m'
    return f'{s//3600}h{(s%3600)//60}m'

def age_min(t: Token) -> float:
    return (time.time() - t.created_at) / 60

# =========================================================
# SCORE-BASED FILTER
# =========================================================
def compute_score(t: Token) -> tuple[int, list[str]]:
    """Returns (score, [reason_strings])."""
    score   = 0
    reasons = []
    ps = pool_sol(t)
    p  = pressure(t)
    b  = bpm(t)
    bc = t.bonding_curve

    # --- pool depth (replaces net SOL) ---
    if ps >= 50:
        score += 35; reasons.append(f'+35 pool_sol>=50 ({ps:.1f})')
    elif ps >= 30:
        score += 25; reasons.append(f'+25 pool_sol>=30 ({ps:.1f})')
    elif ps >= 15:
        score += 15; reasons.append(f'+15 pool_sol>=15 ({ps:.1f})')

    # --- buy pressure ---
    if p >= 75:
        score += 20; reasons.append(f'+20 pressure>=75 ({p}%)')
    elif p >= 60:
        score += 10; reasons.append(f'+10 pressure>=60 ({p}%)')

    # --- buy velocity ---
    if b >= 10:
        score += 20; reasons.append(f'+20 bpm>=10 ({b:.1f})')
    elif b >= 5:
        score += 10; reasons.append(f'+10 bpm>=5 ({b:.1f})')

    # --- bonding curve sweet spot ---
    if BC_MIN <= bc <= BC_MAX:
        score += 15; reasons.append(f'+15 bc={bc:.0f}%')

    # --- socials ---
    if t.twitter or t.telegram_link or t.website:
        score += 10; reasons.append('+10 has socials')

    # --- penalties ---
    if t.sell_count > t.buy_count:
        score -= 25; reasons.append('-25 sells>buys')
    if b < 2:
        score -= 20; reasons.append(f'-20 bpm<2 ({b:.1f})')

    return score, reasons

def passes_hard(t: Token) -> tuple[bool, str]:
    """Hard gates that must pass before scoring."""
    if age_min(t) > MAX_AGE_MINUTES:
        return False, f'Too old ({age_min(t):.0f}m)'
    if t.market_cap < MC_MIN:
        return False, f'MC too low ({fmt(t.market_cap)})'
    if t.market_cap > MC_MAX:
        return False, f'MC too high ({fmt(t.market_cap)})'
    ps = pool_sol(t)
    if ps < MIN_POOL_SOL:
        return False, f'Pool SOL too low ({ps:.2f} SOL)'
    if pressure(t) < MIN_PRESSURE:
        return False, f'Pressure too low ({pressure(t)}%)'
    if bpm(t) < MIN_BPM:
        return False, f'BPM too low ({bpm(t):.1f})'
    if t.buy_count < MIN_BUYS:
        return False, f'Not enough buys ({t.buy_count})'
    return True, 'OK'

def passes(t: Token) -> tuple[bool, str]:
    ok, reason = passes_hard(t)
    if not ok:
        return False, reason
    score, _ = compute_score(t)
    t.last_score = score
    if score < MIN_SCORE:
        return False, f'Score too low ({score}/{MIN_SCORE})'
    return True, f'Score={score}'

# =========================================================
# SECURITY READ
# =========================================================
def security_read(t: Token) -> list:
    liq = get_liq(t)
    reads = []
    p = pressure(t)
    b = bpm(t)
    if p >= 70:   reads.append('Strong buy pressure')
    elif p >= 55: reads.append('Moderate buy pressure')
    else:         reads.append('Sell pressure present')
    if liq >= 10_000:  reads.append('Good liquidity')
    elif liq >= 4_000: reads.append('Decent liquidity')
    else:              reads.append('Low liquidity')
    if b >= 5: reads.append('High buy frequency')
    if t.migrated:
        reads.append('Migrated to Raydium')
    elif t.bonding_curve > 0:
        reads.append(f'Bonding curve {t.bonding_curve:.0f}%')
    if age_min(t) < 5:   reads.append('Very early launch')
    elif age_min(t) < 15: reads.append('Early stage')
    if t.twitter or t.telegram_link or t.website:
        reads.append('Has socials')
    else:
        reads.append('No socials found')
    return reads[:4]

# =========================================================
# ALERT FORMAT
# =========================================================
def build_alert(t: Token) -> str:
    liq  = get_liq(t)
    src  = liq_source(t)
    p    = pressure(t)
    ps   = pool_sol(t)
    vol  = (t.sol_in + t.sol_out) * SOL_PRICE   # volume still uses sol_in/sol_out
    sec  = security_read(t)
    socials = []
    if t.twitter:       socials.append(f'[X]({t.twitter})')
    if t.telegram_link: socials.append(f'[TG]({t.telegram_link})')
    if t.website:       socials.append(f'[Web]({t.website})')
    soc      = ' | '.join(socials) if socials else 'None'
    sec_fmt  = '\n'.join(f'- {s}' for s in sec)
    score_str = f'{t.last_score}' if t.last_score else '?'
    return (
        f'*GEM DETECTED* \\[Score: {score_str}\\]\n\n'
        f'*{t.name}* (${t.symbol})\n'
        f'Age: {age_str(t)}\n\n'
        f'Market Cap  : {fmt(t.market_cap)}\n'
        f'Pool Depth  : {ps:.1f} SOL ({fmt(liq)} {src})\n'
        f'Volume      : {fmt(vol)}\n'
        f'Buy / Sell  : {t.buy_count} / {t.sell_count}\n'
        f'Pressure    : {p}%\n'
        f'BPM         : {bpm(t):.1f}\n'
        f'BC Progress : {t.bonding_curve:.0f}%\n\n'
        f'Security\n{sec_fmt}\n\n'
        f'Socials: {soc}\n\n'
        f'`{t.mint}`'
    )

# =========================================================
# RAYDIUM LIQ FETCH
# =========================================================
async def fetch_raydium_liq(mint: str) -> float:
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f'https://api.dexscreener.com/latest/dex/tokens/{mint}')
            data = r.json()
            pairs = [p for p in (data.get('pairs') or []) if p.get('chainId') == 'solana']
            if pairs:
                best = max(pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0)))
                liq  = float((best.get('liquidity') or {}).get('usd', 0) or 0)
                if liq > 0:
                    return liq
    except Exception as e:
        log.debug(f'Raydium liq fetch failed: {e}')
    return 0.0

# =========================================================
# SOCIALS FROM PUMP.FUN METADATA
# =========================================================
async def fetch_pump_socials(mint: str) -> dict:
    try:
        async with httpx.AsyncClient(timeout=8) as c:
            r = await c.get(f'https://frontend-api.pump.fun/coins/{mint}')
            data = r.json()
            return {
                'twitter':  data.get('twitter', ''),
                'telegram': data.get('telegram', ''),
                'website':  data.get('website', ''),
            }
    except Exception as e:
        log.debug(f'Pump socials fetch failed: {e}')
    return {}

# =========================================================
# SEND ALERT
# t.called = True is set ONLY after a successful Telegram
# send — a failed send leaves the token eligible for retry.
# =========================================================
async def send_alert(app: Application, t: Token) -> None:
    await asyncio.sleep(ALERT_DELAY_SEC)

    ok, reason = passes(t)
    if not ok:
        log.info(f'ALERT BLOCKED after delay: {t.name} ({t.symbol}) | {reason}')
        return

    dex    = f'https://dexscreener.com/solana/{t.mint}'
    photon = f'https://photon-sol.tinyastro.io/en/lp/{t.mint}'
    bullx  = f'https://bullx.io/terminal?chainId=1399811149&address={t.mint}'
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton('Photon', url=photon),
        InlineKeyboardButton('BullX',  url=bullx),
        InlineKeyboardButton('Dex',    url=dex),
    ]])
    try:
        await app.bot.send_message(
            chat_id=CHAT_ID,
            text=build_alert(t),
            parse_mode='Markdown',
            disable_web_page_preview=True,
            reply_markup=kb,
        )
        t.called     = True
        t.entry_mc   = t.market_cap
        t.next_milestone = 2
        recent_calls.appendleft({
            'name': t.name, 'symbol': t.symbol, 'mint': t.mint,
            'mc': t.market_cap, 'ts': time.time(),
        })
        log.info(
            f'ALERT SENT: {t.name} ({t.symbol}) | '
            f'MC={fmt(t.market_cap)} | Liq={fmt(get_liq(t))} | '
            f'PoolSOL={pool_sol(t):.1f} | Score={t.last_score}'
        )
    except Exception as e:
        log.error(f'Alert send error: {e}')

# =========================================================
# MILESTONES
# =========================================================
async def check_milestones(app: Application, t: Token) -> None:
    if not t.called or not t.entry_mc or not t.next_milestone:
        return
    if t.market_cap <= 0:
        return
    mult = t.market_cap / t.entry_mc
    if mult >= t.next_milestone:
        m      = t.next_milestone
        next_m = next((x for x in MILESTONES if x > m), None)
        t.next_milestone = next_m
        try:
            await app.bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f'*{m}x HIT*\n\n'
                    f'*{t.name}* (${t.symbol})\n'
                    f'Entry : {fmt(t.entry_mc)}\n'
                    f'Now   : {fmt(t.market_cap)}\n'
                    f'*{mult:.1f}x* from call\n\n'
                    f'`{t.mint}`'
                ),
                parse_mode='Markdown',
            )
        except Exception as e:
            log.error(f'Milestone error: {e}')

# =========================================================
# CA SCAN (manual lookup via Telegram message)
# =========================================================
async def scan_ca(ca: str) -> str:
    if ca in tokens:
        t       = tokens[ca]
        liq     = get_liq(t)
        ok, reason = passes(t)
        score, score_reasons = compute_score(t)
        vol     = (t.sol_in + t.sol_out) * SOL_PRICE
        socials = []
        if t.twitter:       socials.append(f'[X]({t.twitter})')
        if t.telegram_link: socials.append(f'[TG]({t.telegram_link})')
        if t.website:       socials.append(f'[Web]({t.website})')
        soc     = ' | '.join(socials) if socials else 'None'
        sec     = security_read(t)
        sec_fmt = '\n'.join(f'- {s}' for s in sec)
        dex     = f'https://dexscreener.com/solana/{ca}'
        photon  = f'https://photon-sol.tinyastro.io/en/lp/{ca}'
        return (
            f'*{t.name}* (${t.symbol})\n'
            f'Age: {age_str(t)}\n\n'
            f'Market Cap  : {fmt(t.market_cap)}\n'
            f'Pool Depth  : {pool_sol(t):.2f} SOL ({fmt(liq)} {liq_source(t)})\n'
            f'Volume      : {fmt(vol)}\n'
            f'Buy / Sell  : {t.buy_count} / {t.sell_count}\n'
            f'Pressure    : {pressure(t)}%\n'
            f'BPM         : {bpm(t):.1f}\n'
            f'BC Progress : {t.bonding_curve:.0f}%\n\n'
            f'Security\n{sec_fmt}\n\n'
            f'Socials: {soc}\n\n'
            f'Score  : {score}/{MIN_SCORE} | Filter: {"PASS" if ok else reason}\n\n'
            f'[Dex]({dex}) | [Photon]({photon})\n'
            f'`{ca}`'
        )

    # Fallback: hit DexScreener directly
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r    = await c.get(f'https://api.dexscreener.com/latest/dex/tokens/{ca}')
            data = r.json()
            pairs = [p for p in (data.get('pairs') or []) if p.get('chainId') == 'solana']
            if pairs:
                pair   = max(pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0)))
                base   = pair.get('baseToken', {})
                mc     = float(pair.get('fdv') or 0)
                liq    = float((pair.get('liquidity') or {}).get('usd', 0) or 0)
                vol24  = float((pair.get('volume') or {}).get('h24', 0) or 0)
                buys1  = int((pair.get('txns') or {}).get('h1', {}).get('buys', 0))
                sells1 = int((pair.get('txns') or {}).get('h1', {}).get('sells', 0))
                total  = buys1 + sells1
                pres   = int(buys1 / total * 100) if total else 0
                pc1h   = float((pair.get('priceChange') or {}).get('h1', 0) or 0)
                socials_d = []
                for s in (pair.get('info') or {}).get('socials', []):
                    url = s.get('url', '')
                    if url: socials_d.append(f'[{s.get("type","Link").capitalize()}]({url})')
                soc = ' | '.join(socials_d) if socials_d else 'None'
                reads = []
                if pres >= 65:  reads.append('Strong buy pressure')
                elif pres >= 50: reads.append('Moderate buy pressure')
                else:            reads.append('Sell pressure present')
                if liq >= 10_000:  reads.append('Good liquidity')
                elif liq >= 3_000: reads.append('Decent liquidity')
                else:              reads.append('Low liquidity')
                if pc1h > 20:   reads.append('Strong 1h momentum')
                elif pc1h < -20: reads.append('Dumping last hour')
                if mc < 30_000: reads.append('Very early stage')
                sec_fmt = '\n'.join(f'- {r}' for r in reads[:4])
                dex    = f'https://dexscreener.com/solana/{ca}'
                photon = f'https://photon-sol.tinyastro.io/en/lp/{ca}'
                bullx  = f'https://bullx.io/terminal?chainId=1399811149&address={ca}'
                return (
                    f'*{base.get("name","?")}* (${base.get("symbol","?")})\n\n'
                    f'Market Cap : {fmt(mc)}\n'
                    f'Liq Pool   : {fmt(liq)}\n'
                    f'Volume     : {fmt(vol24)}\n'
                    f'Buy / Sell : {buys1} / {sells1} (1h)\n'
                    f'Pressure   : {pres}%\n\n'
                    f'Security\n{sec_fmt}\n\n'
                    f'Socials: {soc}\n\n'
                    f'[Dex]({dex}) | [Photon]({photon}) | [BullX]({bullx})\n'
                    f'`{ca}`'
                )
    except Exception as e:
        log.warning(f'scan_ca DexScreener error: {e}')
    return (
        f'No data found for `{ca}`\n\n'
        f'Token may be too new.\n'
        f'[Check DexScreener](https://dexscreener.com/solana/{ca})'
    )

# =========================================================
# COMMANDS
# =========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        '*GemStalker* is live\n\n'
        'Scanning pump.fun websocket in real-time.\n'
        'Paste any Solana CA to analyse it.\n\n'
        '/status  - live stats\n'
        '/calls   - this month calls\n'
        '/filters - active filters\n'
        '/debug   - top watchlist candidates',
        parse_mode='Markdown',
    )

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    total    = len(tokens)
    alerted  = sum(1 for t in tokens.values() if t.called)
    watching = sum(1 for t in tokens.values() if t.watchlist and not t.called)
    top = sorted(
        [t for t in tokens.values() if not t.called and t.buy_count >= 2],
        key=lambda t: t.last_score, reverse=True
    )[:3]
    msg = (
        '*GemStalker Status*\n\n'
        f'Tokens tracked : {total}\n'
        f'Watchlist      : {watching}\n'
        f'Alerts sent    : {alerted}\n'
        f'SOL Price      : ${SOL_PRICE:.2f}\n'
    )
    if top:
        msg += '\n*Top candidates (by score):*\n'
        for t in top:
            ok, reason = passes(t)
            msg += (
                f'\n*{t.name}* (${t.symbol})\n'
                f'MC: {fmt(t.market_cap)} | PoolSOL: {pool_sol(t):.1f} | Score: {t.last_score}\n'
                f'{"✅ PASS" if ok else f"❌ {reason}"}\n'
            )
    await update.message.reply_text(msg, parse_mode='Markdown')

async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show why the top tokens are not alerting."""
    top = sorted(
        [t for t in tokens.values() if not t.called],
        key=lambda t: t.last_score, reverse=True
    )[:5]
    if not top:
        await update.message.reply_text('No tracked tokens yet.')
        return
    lines = ['*Debug - Top 5 candidates*\n']
    for t in top:
        ok, reason = passes(t)
        score, score_reasons = compute_score(t)
        lines.append(
            f'*{t.name}* | Score {score} | Age {age_str(t)}\n'
            f'MC={fmt(t.market_cap)} PoolSOL={pool_sol(t):.1f} Pressure={pressure(t)}% BPM={bpm(t):.1f}\n'
            f'{"✅ PASS" if ok else f"❌ {reason}"}\n'
            f'_{" | ".join(score_reasons[:3])}_\n'
        )
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')

async def cmd_calls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_calls:
        await update.message.reply_text('No calls this month yet.')
        return
    month_start = time.mktime(time.strptime(time.strftime('%Y-%m-01'), '%Y-%m-%d'))
    month_calls = [c for c in recent_calls if c['ts'] >= month_start]
    if not month_calls:
        await update.message.reply_text('No calls this month yet.')
        return
    month_name = time.strftime('%B %Y')
    lines = [f'*Calls - {month_name}* ({len(month_calls)} total)\n']
    for i, c in enumerate(month_calls, 1):
        mint     = c['mint']
        entry_mc = c['mc']
        mult_str = 'tracking...'
        if mint in tokens:
            t = tokens[mint]
            if t.market_cap > 0 and entry_mc > 0:
                mult_str = f'{t.market_cap / entry_mc:.1f}x'
        lines.append(
            f'{i}. *{c["name"]}* (${c["symbol"]})\n'
            f'   Entry: {fmt(entry_mc)} | Now: {mult_str}'
        )
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')

async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        '*Active Filters*\n\n'
        f'MC Range     : {fmt(MC_MIN)} – {fmt(MC_MAX)}\n'
        f'Min Pool SOL : {MIN_POOL_SOL} SOL\n'
        f'Min Pressure : {MIN_PRESSURE}%\n'
        f'Min BPM      : {MIN_BPM}\n'
        f'Min Buys     : {MIN_BUYS}\n'
        f'Max Age      : {MAX_AGE_MINUTES}m\n'
        f'BC Range     : {BC_MIN}% – {BC_MAX}%\n'
        f'Min Score    : {MIN_SCORE}\n\n'
        f'SOL Price    : ${SOL_PRICE:.2f}',
        parse_mode='Markdown',
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or '').strip()
    if SOLANA_CA_RE.fullmatch(text):
        ca = text
    else:
        matches = SOLANA_CA_RE.findall(text)
        if not matches:
            return
        ca = matches[0]
    msg    = await update.message.reply_text(f'Scanning {ca[:12]}...')
    result = await scan_ca(ca)
    try:
        await msg.edit_text(result, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception:
        await msg.edit_text(result, disable_web_page_preview=True)

# =========================================================
# EVENT VALIDATION & NORMALISATION
# =========================================================
def validate_and_normalize_event(raw_msg: dict) -> tuple[bool, dict | None]:
    """
    Validates and normalises an incoming websocket event.
    Key fix: solAmount is in lamports → divide by 1e9 here.
    virtualSolReserves is also in lamports → divide by 1e9 here.
    bondingCurveProgress arrives as 0-100 → kept as-is (0-100).
    """
    if not raw_msg.get('mint'):
        return False, None

    tx_type = (raw_msg.get('txType') or '').lower()
    if tx_type not in ('buy', 'sell', 'create', 'new', 'mint'):
        if tx_type:
            log.debug(f'Unknown txType: {tx_type}')
        return False, None

    normalized_tx = 'buy' if tx_type == 'buy' else 'sell' if tx_type == 'sell' else 'create'

    # bondingCurveProgress: handle both 0-1 and 0-100 scales defensively
    bc_raw = float(raw_msg.get('bondingCurveProgress') or 0)
    bc_pct = bc_raw if bc_raw > 1 else bc_raw * 100   # normalise to 0-100

    normalized = {
        'mint':                 raw_msg['mint'].strip(),
        'txType':               normalized_tx,
        'name':                 (raw_msg.get('name') or 'Unknown').strip()[:100],
        'symbol':               (raw_msg.get('symbol') or '?').strip()[:20],
        # ---- CRITICAL FIX: both amounts arrive in lamports ----
        'solAmount':            max(float(raw_msg.get('solAmount') or 0), 0) / 1e9,
        'virtualSolReserves':   max(float(raw_msg.get('virtualSolReserves') or 0), 0) / 1e9,
        # -------------------------------------------------------
        'marketCapSol':         max(float(raw_msg.get('marketCapSol') or 0), 0),
        'bondingCurveProgress': bc_pct,
        'raydiumPool':          raw_msg.get('raydiumPool') or '',
        'twitter':              (raw_msg.get('twitter') or '').strip(),
        'telegram':             (raw_msg.get('telegram') or '').strip(),
        'website':              (raw_msg.get('website') or '').strip(),
    }
    return True, normalized

# =========================================================
# HANDLE EVENT
# =========================================================
async def handle_event(app: Application, normalized: dict) -> None:
    global ev_count, buy_count_global

    tx_type = normalized['txType']
    mint    = normalized['mint']
    ev_count += 1

    ws_event_counts[tx_type] = ws_event_counts.get(tx_type, 0) + 1

    if mint not in tokens:
        tokens[mint] = Token(mint=mint)

    t = tokens[mint]
    t.last_active = time.time()
    t.name   = normalized['name']   or t.name
    t.symbol = normalized['symbol'] or t.symbol

    # Market cap (SOL → USD)
    if normalized['marketCapSol'] > 0:
        t.market_cap = normalized['marketCapSol'] * SOL_PRICE

    # Virtual SOL reserves = real pool depth (already in SOL after normalisation)
    if normalized['virtualSolReserves'] > 0:
        t.virtual_sol = normalized['virtualSolReserves']

    # Bonding curve 0-100
    t.bonding_curve = min(normalized['bondingCurveProgress'], 100.0)

    sol_amt = normalized['solAmount']   # already in SOL

    if tx_type == 'buy':
        buy_count_global += 1
        t.buy_count += 1
        t.sol_in    += sol_amt
        t.buys_ts.append(time.time())
        log.debug(f'BUY: {t.name} | +{sol_amt:.4f} SOL | pool={t.virtual_sol:.2f} SOL')
    elif tx_type == 'sell':
        t.sell_count += 1
        t.sol_out    += sol_amt
        log.debug(f'SELL: {t.name} | -{sol_amt:.4f} SOL | pool={t.virtual_sol:.2f} SOL')

    # Socials
    for field_name, attr in [('twitter', 'twitter'), ('telegram', 'telegram_link'), ('website', 'website')]:
        val = normalized.get(field_name, '')
        if val and not getattr(t, attr):
            setattr(t, attr, val)

    # Migration
    if normalized['raydiumPool'] and not t.migrated:
        t.migrated = True
        log.info(f'Migration detected: {t.name} ({t.symbol})')
        asyncio.create_task(fetch_and_set_raydium_liq(t))

    # Already alerted — track milestones only
    if t.called:
        await check_milestones(app, t)
        return

    # Score & filter
    score, score_reasons = compute_score(t)
    t.last_score = score

    ok, reason = passes(t)
    if ok:
        if not t.twitter and not t.telegram_link and not t.website:
            asyncio.create_task(fetch_and_set_socials(app, t))
        else:
            asyncio.create_task(send_alert(app, t))
    else:
        if tx_type == 'buy' and t.buy_count % 10 == 0:
            log.info(
                f'SKIP: {t.name} ({t.symbol}) | '
                f'Score={score}/{MIN_SCORE} | {reason} | '
                f'MC={fmt(t.market_cap)} | PoolSOL={pool_sol(t):.2f} | '
                f'Buys={t.buy_count} | Pressure={pressure(t)}% | BPM={bpm(t):.1f}'
            )

async def fetch_and_set_raydium_liq(t: Token) -> None:
    liq = await fetch_raydium_liq(t.mint)
    if liq > 0:
        t.raydium_liq = liq
        log.info(f'Raydium liq set: {fmt(liq)} for {t.name}')

async def fetch_and_set_socials(app: Application, t: Token) -> None:
    socials = await fetch_pump_socials(t.mint)
    if socials.get('twitter'):  t.twitter       = socials['twitter']
    if socials.get('telegram'): t.telegram_link = socials['telegram']
    if socials.get('website'):  t.website       = socials['website']
    await send_alert(app, t)

# =========================================================
# WEBSOCKET LOOP
# =========================================================
async def websocket_loop(app: Application) -> None:
    retries        = 0
    connection_uri = PUMP_WS + (f'?api-key={PUMPPORTAL_KEY}' if PUMPPORTAL_KEY else '')

    while True:
        try:
            log.info(f'WS connecting (attempt {retries+1})')
            async with websockets.connect(
                connection_uri,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=10,
            ) as ws:
                retries = 0
                log.info('WS connected to pumpportal.fun')

                await ws.send(json.dumps({'method': 'subscribeNewToken'}))
                log.info('WS subscription sent: subscribeNewToken')

                hb    = time.time()
                stats = {'received': 0, 'valid': 0, 'invalid': 0, 'by_type': {}}

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if not msg:
                            stats['invalid'] += 1
                            continue

                        stats['received'] += 1
                        tx_type = msg.get('txType', 'unknown')
                        stats['by_type'][tx_type] = stats['by_type'].get(tx_type, 0) + 1

                        is_valid, normalized = validate_and_normalize_event(msg)
                        if not is_valid:
                            stats['invalid'] += 1
                            continue

                        stats['valid'] += 1

                        # Sample log for first few events of each type
                        if stats['by_type'].get(normalized['txType'], 0) <= 3:
                            log.debug(
                                f'[WS SAMPLE] txType={normalized["txType"]} '
                                f'mint={normalized["mint"][:12]} '
                                f'solAmount={normalized["solAmount"]:.4f} SOL '
                                f'virtualSol={normalized["virtualSolReserves"]:.2f} SOL '
                                f'marketCapSol={normalized["marketCapSol"]:.2f} '
                                f'bc={normalized["bondingCurveProgress"]:.1f}%'
                            )

                        # Per-token trade subscription for new tokens
                        if normalized['txType'] == 'create':
                            mint = normalized['mint']
                            if mint not in subscribed_tokens and len(subscribed_tokens) < MAX_SUBSCRIPTIONS:
                                try:
                                    await ws.send(json.dumps({
                                        'method': 'subscribeTokenTrade',
                                        'keys':   [mint]
                                    }))
                                    subscribed_tokens.add(mint)
                                    log.info(
                                        f'Subscribed: {normalized["name"]} ({normalized["symbol"]}) '
                                        f'| {mint[:12]} | total={len(subscribed_tokens)}'
                                    )
                                except Exception as e:
                                    log.error(f'Subscribe error {mint}: {e}')

                        await handle_event(app, normalized)

                        # Heartbeat every 60 s
                        if time.time() - hb > 60:
                            hb = time.time()
                            alerted = sum(1 for t in tokens.values() if t.called)
                            log.info(
                                f'WS alive | tokens={len(tokens)} alerts={alerted} '
                                f'subscribed={len(subscribed_tokens)} | '
                                f'recv={stats["received"]} valid={stats["valid"]} invalid={stats["invalid"]} | '
                                f'types={dict(list(stats["by_type"].items())[:8])}'
                            )
                            stats = {'received': 0, 'valid': 0, 'invalid': 0, 'by_type': {}}

                    except json.JSONDecodeError as e:
                        log.warning(f'JSON decode error: {e} | raw={raw[:100]}')
                    except Exception as e:
                        log.error(f'Event handling error: {e}', exc_info=False)

        except Exception as e:
            retries += 1
            wait = min(5 * retries, 60)
            log.error(f'WS disconnected: {e} — retrying in {wait}s (attempt {retries})')
            await asyncio.sleep(wait)

# =========================================================
# CLEANUP & STATS
# =========================================================
async def cleanup_tokens() -> None:
    while True:
        await asyncio.sleep(300)
        cutoff = time.time() - 3600
        stale  = [m for m, t in tokens.items() if t.last_active < cutoff and not t.called]
        for m in stale:
            del tokens[m]
        if stale:
            log.info(f'Cleaned {len(stale)} stale tokens')

async def log_stats() -> None:
    global ev_count, buy_count_global
    while True:
        await asyncio.sleep(60)
        alerted  = sum(1 for t in tokens.values() if t.called)
        watching = sum(1 for t in tokens.values() if t.watchlist and not t.called)
        log.info(
            f'STATS | ev={ev_count} buys={buy_count_global} '
            f'tokens={len(tokens)} watching={watching} alerts={alerted} '
            f'ws_events={dict(list(ws_event_counts.items())[:6])}'
        )
        ev_count         = 0
        buy_count_global = 0

# =========================================================
# HEALTH SERVER
# =========================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'GemStalker OK')

    def log_message(self, *args):
        pass

def run_health():
    HTTPServer(('0.0.0.0', int(os.getenv('PORT', 8080))), HealthHandler).serve_forever()

# =========================================================
# STARTUP & MAIN
# =========================================================
async def post_init(app: Application) -> None:
    log.info('Starting background tasks')
    asyncio.create_task(update_sol_price())
    asyncio.create_task(websocket_loop(app))
    asyncio.create_task(cleanup_tokens())
    asyncio.create_task(log_stats())
    log.info('GemStalker ready — pool-depth liquidity active')

def main() -> None:
    if not TG_TOKEN: raise RuntimeError('TELEGRAM_BOT_TOKEN not set')
    if not CHAT_ID:  raise RuntimeError('CHAT_ID not set')
    if not PUMPPORTAL_KEY:
        log.warning('PUMPPORTAL_API_KEY not set — connection may be rate-limited')

    threading.Thread(target=run_health, daemon=True).start()
    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler('start',   cmd_start))
    app.add_handler(CommandHandler('status',  cmd_status))
    app.add_handler(CommandHandler('calls',   cmd_calls))
    app.add_handler(CommandHandler('filters', cmd_filters))
    app.add_handler(CommandHandler('debug',   cmd_debug))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.post_init = post_init
    log.info('GemStalker starting')
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
