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
CHAT_ID  = os.getenv('CHAT_ID')
PUMP_WS  = 'wss://pumpportal.fun/api/data'
SOL_PRICE = 150.0
SOLANA_CA_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')
# =========================================================
# FILTERS
# =========================================================
MC_MIN            = 5_000
MC_MAX            = 200_000
MIN_LIQ_USD       = 4_000
MIN_LIQ_RATIO     = 0.15
MIN_SOL_IN        = 1.0
MIN_BUYS          = 5
MIN_BUYS_PER_MIN  = 1.0
MAX_AGE_MINUTES   = 30
MAX_TOP10_PCT     = 35.0
MAX_DEV_PCT       = 5.0
MILESTONES = [2, 5, 10, 25, 50, 100]
# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
# =========================================================
# DATA MODEL
# =========================================================
@dataclass
class Token:
    mint: str
    name:            str   = 'Unknown'
    symbol:          str   = '?'
    market_cap:      float = 0.0
    sol_reserves:    float = 0.0
    token_reserves:  float = 0.0
    sol_in:          float = 0.0
    sol_out:         float = 0.0
    bonding_curve:   float = 0.0
    holders:         int   = 0
    top_holder:      float = 0.0
    buy_count:       int   = 0
    sell_count:      int   = 0
    buy_volume:      float = 0.0
    sell_volume:     float = 0.0
    migrated:        bool  = False
    raydium_liq:     float = 0.0
    called:          bool  = False
    twitter:         str   = ''
    telegram:        str   = ''
    website:         str   = ''
    created_at:      float = field(default_factory=time.time)
    last_active:     float = field(default_factory=time.time)
    buys:            deque = field(default_factory=lambda: deque(maxlen=500))
    entry_mc:        float = 0.0
    next_milestone:  int   = 2
tokens: dict        = {}
recent_calls: deque = deque(maxlen=500)
event_counter = 0
buy_counter   = 0
# =========================================================
# LIQUIDITY CALCULATION
# =========================================================
def calc_bonding_curve_liq(t: Token) -> float:
    return t.sol_reserves * SOL_PRICE
def get_confirmed_liq(t: Token) -> float:
    if t.migrated and t.raydium_liq > 0:
        return t.raydium_liq
    bc_liq = calc_bonding_curve_liq(t)
    if bc_liq > 0:
        return bc_liq
    if t.sol_in > 0:
        return t.sol_in * SOL_PRICE * 0.8
    return 0.0
def liq_confirmed(t: Token) -> bool:
    liq = get_confirmed_liq(t)
    if liq < MIN_LIQ_USD:
        return False
    if t.market_cap > 0:
        ratio = liq / t.market_cap
        if ratio < MIN_LIQ_RATIO:
            return False
    return True
# =========================================================
# SOL PRICE
# =========================================================
async def update_sol_price() -> None:
    global SOL_PRICE
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd'
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                r = await client.get(url)
                data = r.json()
                SOL_PRICE = float(data['solana']['usd'])
                log.info(f'SOL price: ${SOL_PRICE:.2f}')
        except Exception as e:
            log.warning(f'SOL price update failed: {e}')
        await asyncio.sleep(60)
# =========================================================
# HELPERS
# =========================================================
def fmt(n, decimals=2) -> str:
    if n is None:
        return '?'
    n = float(n)
    if n >= 1_000_000:
        return f'${n/1_000_000:.{decimals}f}M'
    if n >= 1_000:
        return f'${n/1_000:.{decimals}f}K'
    return f'${n:.{decimals}f}'
def buys_per_min(t: Token) -> float:
    cutoff = time.time() - 60
    return float(sum(1 for x in t.buys if x >= cutoff))
def buy_pressure(t: Token) -> int:
    total = t.buy_count + t.sell_count
    if total == 0:
        return 0
    return int((t.buy_count / total) * 100)
def token_age_min(t: Token) -> float:
    return (time.time() - t.created_at) / 60
def token_age_str(t: Token) -> str:
    secs = int(time.time() - t.created_at)
    if secs < 60:   return f'{secs}s'
    if secs < 3600: return f'{secs // 60}m'
    return f'{secs // 3600}h {(secs % 3600) // 60}m'
def alpha_score(t: Token) -> float:
    liq   = get_confirmed_liq(t)
    score = 0.0
    score += min(t.sol_in * 0.3, 2.5)
    score += min(buys_per_min(t) * 0.3, 2.0)
    score += min(t.holders / 50, 1.5)
    if buy_pressure(t) > 65: score += 1.0
    if t.top_holder and t.top_holder < 15: score += 0.5
    if liq >= MIN_LIQ_USD * 2: score += 1.0
    if t.migrated: score += 1.0
    if t.twitter or t.telegram or t.website: score += 0.5
    return round(min(score, 10), 1)
# =========================================================
# FILTER
# =========================================================
def passes_filters(t: Token) -> tuple:
    age = token_age_min(t)
    if age > MAX_AGE_MINUTES:
        return False, f'Too old ({age:.0f}m > {MAX_AGE_MINUTES}m)'
    if t.market_cap < MC_MIN:
        return False, f'MC too low ({fmt(t.market_cap)})'
    if t.market_cap > MC_MAX:
        return False, f'MC too high ({fmt(t.market_cap)})'
    liq = get_confirmed_liq(t)
    if liq < MIN_LIQ_USD:
        return False, f'Liq too low ({fmt(liq)} < {fmt(MIN_LIQ_USD)})'
    if t.market_cap > 0 and liq / t.market_cap < MIN_LIQ_RATIO:
        return False, f'Liq/MC ratio {liq/t.market_cap:.1%} too low (min {MIN_LIQ_RATIO:.0%})'
    if t.sol_in < MIN_SOL_IN:
        return False, f'Buy inflow too low ({t.sol_in:.2f} < {MIN_SOL_IN} SOL)'
    if t.buy_count < MIN_BUYS:
        return False, f'Not enough buys ({t.buy_count} < {MIN_BUYS})'
    bpm = buys_per_min(t)
    if bpm < MIN_BUYS_PER_MIN:
        return False, f'Low buy rate ({bpm:.2f}/min)'
    if t.top_holder and t.top_holder > MAX_TOP10_PCT:
        return False, f'Top holder {t.top_holder:.1f}% too high'
    net_flow = t.sol_in - t.sol_out
    if net_flow < 0:
        return False, f'Net negative flow ({net_flow:.2f} SOL)'
    return True, 'OK'
# =========================================================
# RAYDIUM LIQ FETCH
# =========================================================
async def fetch_raydium_liq(mint: str) -> float:
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            url = f'https://api.dexscreener.com/latest/dex/tokens/{mint}'
            r   = await client.get(url)
            data = r.json()
            pairs = data.get('pairs') or []
            sol_pairs = [p for p in pairs if p.get('chainId') == 'solana']
            if sol_pairs:
                best = max(sol_pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0) or 0))
                liq = float((best.get('liquidity') or {}).get('usd', 0) or 0)
                if liq > 0:
                    return liq
    except Exception as e:
        log.debug(f'Raydium liq fetch failed for {mint[:8]}: {e}')
    return 0.0
# =========================================================
# ALERT MESSAGE
# =========================================================
def build_alert(t: Token, tag: str = 'GEM DETECTED') -> str:
    liq      = get_confirmed_liq(t)
    pressure = buy_pressure(t)
    net_flow = t.sol_in - t.sol_out
    score    = alpha_score(t)
    bpm      = buys_per_min(t)
    liq_src  = 'Raydium' if t.migrated else 'Bonding Curve'
    if score >= 7:    label = 'RUNNER'
    elif score >= 5:  label = 'WATCHLIST'
    elif score >= 3:  label = 'SPECULATIVE'
    else:             label = 'AVOID'
    reads = []
    if pressure > 68:           reads.append('Buyers aggressive')
    elif pressure < 35:         reads.append('Sell pressure present')
    if bpm > 5:                 reads.append('High buy frequency')
    if net_flow > 0:            reads.append('Net positive buy flow')
    elif net_flow < -0.5:       reads.append('Net outflow caution')
    if t.sol_in > 5:            reads.append('Strong accumulation')
    if t.migrated:              reads.append('Migrated to Raydium')
    if not reads:               reads.append('Momentum building')
    reads = reads[:4]
    socials = []
    if t.twitter:  socials.append(f'[X]({t.twitter})')
    if t.telegram: socials.append(f'[TG]({t.telegram})')
    if t.website:  socials.append(f'[Web]({t.website})')
    soc_line = ' | '.join(socials) if socials else 'None'
    reads_fmt = '\n'.join(f'- {r}' for r in reads)
    return (
        f'*{tag}*\n\n'
        f'*{t.name}* ({t.symbol})\n'
        f'Age: {token_age_str(t)}\n\n'
        f'MC       : {fmt(t.market_cap)}\n'
        f'LIQ      : {fmt(liq)} ({liq_src})\n'
        f'LIQ/MC   : {liq/t.market_cap:.1%}\n'
        f'INFLOW   : {t.sol_in:.2f} SOL\n'
        f'NET FLOW : {net_flow:+.2f} SOL\n'
        f'PRESSURE : {pressure}%\n\n'
        f'BUY/SELL : {t.buy_count} / {t.sell_count}\n'
        f'B/MIN    : {bpm:.1f}\n'
        f'HOLDERS  : {t.holders if t.holders else "N/A"}\n'
        f'TOP HOLD : {f"{t.top_holder:.1f}%" if t.top_holder else "N/A"}\n\n'
        f'AI READ\n{reads_fmt}\n\n'
        f'SOCIALS: {soc_line}\n\n'
        f'SCORE: {score}/10 | {label}\n\n'
        f'`{t.mint}`'
    )
# =========================================================
# SEND ALERT
# =========================================================
async def send_alert(app: Application, t: Token) -> None:
    photon = f'https://photon-sol.tinyastro.io/en/lp/{t.mint}'
    bullx  = f'https://bullx.io/terminal?chainId=1399811149&address={t.mint}'
    dex    = f'https://dexscreener.com/solana/{t.mint}'
    keyboard = InlineKeyboardMarkup([[
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
            reply_markup=keyboard,
        )
        t.entry_mc       = t.market_cap
        t.next_milestone = 2
        recent_calls.appendleft({
            'name': t.name, 'symbol': t.symbol, 'mint': t.mint,
            'market_cap': t.market_cap, 'score': alpha_score(t),
            'time': time.time(), 'entry_mc': t.market_cap,
        })
        log.info(f'ALERT: {t.name} ({t.symbol}) MC={fmt(t.market_cap)}')
    except Exception as e:
        log.error(f'Alert send error: {e}')
# =========================================================
# MILESTONE TRACKING
# =========================================================
async def check_milestones(app: Application, t: Token) -> None:
    if not t.called or not t.entry_mc or not t.next_milestone:
        return
    if t.market_cap <= 0:
        return
    mult = t.market_cap / t.entry_mc
    if mult >= t.next_milestone:
        milestone = t.next_milestone
        next_m = next((m for m in MILESTONES if m > milestone), None)
        t.next_milestone = next_m
        try:
            await app.bot.send_message(
                chat_id=CHAT_ID,
                text=(
                    f'*{milestone}x MILESTONE*\n\n'
                    f'*{t.name}* ({t.symbol})\n'
                    f'Entry MC: {fmt(t.entry_mc)}\n'
                    f'Now:      {fmt(t.market_cap)}\n'
                    f'Multiple: *{mult:.1f}x*\n\n'
                    f'`{t.mint}`'
                ),
                parse_mode='Markdown',
            )
            log.info(f'Milestone {milestone}x hit: {t.name}')
        except Exception as e:
            log.error(f'Milestone alert error: {e}')
# =========================================================
# CA ANALYZER
# =========================================================
async def analyze_ca(ca: str) -> str:
    log.info(f'Analyzing CA: {ca}')
    if ca in tokens:
        t   = tokens[ca]
        liq = get_confirmed_liq(t)
        passed, reason = passes_filters(t)
        return (
            f'*{t.name}* ({t.symbol})\n\n'
            f'MC      : {fmt(t.market_cap)}\n'
            f'LIQ     : {fmt(liq)}\n'
            f'INFLOW  : {t.sol_in:.2f} SOL\n'
            f'B/S     : {t.buy_count} / {t.sell_count}\n'
            f'AGE     : {token_age_str(t)}\n'
            f'SCORE   : {alpha_score(t)}/10\n\n'
            f'Filter: {"PASS" if passed else reason}\n\n'
            f'`{ca}`'
        )
    try:
        async with httpx.AsyncClient(timeout=12) as client:
            r    = await client.get(f'https://api.dexscreener.com/latest/dex/tokens/{ca}')
            data = r.json()
            pairs = data.get('pairs') or []
            sol   = [p for p in pairs if p.get('chainId') == 'solana']
            if sol:
                pair   = max(sol, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0) or 0))
                base   = pair.get('baseToken', {})
                mc     = float(pair.get('fdv') or 0)
                liq    = float((pair.get('liquidity') or {}).get('usd', 0) or 0)
                vol1h  = float((pair.get('volume') or {}).get('h1', 0) or 0)
                vol24  = float((pair.get('volume') or {}).get('h24', 0) or 0)
                buys1  = int((pair.get('txns') or {}).get('h1', {}).get('buys', 0))
                sells1 = int((pair.get('txns') or {}).get('h1', {}).get('sells', 0))
                pc1h   = float((pair.get('priceChange') or {}).get('h1', 0) or 0)
                total  = buys1 + sells1
                pres   = int(buys1 / total * 100) if total else 0
                dex    = f'https://dexscreener.com/solana/{ca}'
                photon = f'https://photon-sol.tinyastro.io/en/lp/{ca}'
                bullx  = f'https://bullx.io/terminal?chainId=1399811149&address={ca}'
                flags = []
                if liq < 5_000:             flags.append('Low liquidity')
                if mc > 0 and liq / mc < 0.1: flags.append('Thin liq vs MC')
                if vol24 < 500:             flags.append('Low volume')
                if total > 0 and sells1 > buys1 * 2: flags.append('Heavy sell pressure')
                reads = []
                if pres > 65:   reads.append('Buyers in control')
                if pc1h > 15:   reads.append('Strong 1h momentum')
                elif pc1h < -15: reads.append('Dumping last hour')
                if mc < 50_000: reads.append('Very early stage')
                if not reads:   reads.append('No strong signal')
                if len(flags) == 0 and pres > 60 and vol1h > 3_000:
                    status = 'RUNNER'
                elif len(flags) >= 3 or (total > 5 and sells1 > buys1 * 2):
                    status = 'AVOID'
                elif len(flags) >= 1 or pres < 45:
                    status = 'SPECULATIVE'
                else:
                    status = 'WATCHLIST'
                return (
                    f'*{base.get("name","?")}* ({base.get("symbol","?")})\n\n'
                    f'MC      : {fmt(mc)}\n'
                    f'LIQ     : {fmt(liq)}\n'
                    f'LIQ/MC  : {liq/mc:.1%}\n'
                    f'VOL 1H  : {fmt(vol1h)}\n'
                    f'VOL 24H : {fmt(vol24)}\n'
                    f'B/S     : {buys1} / {sells1} (1h)\n'
                    f'PRESSURE: {pres}%\n'
                    f'CHANGE  : {pc1h:+.1f}% (1h)\n\n'
                    f'FLAGS: {", ".join(flags) if flags else "None"}\n'
                    f'READS: {", ".join(reads)}\n\n'
                    f'STATUS: {status}\n\n'
                    f'[Dex]({dex}) | [Photon]({photon}) | [BullX]({bullx})\n'
                    f'`{ca}`'
                )
    except Exception as e:
        log.warning(f'CA analyze error: {e}')
    return (
        f'No data found for:\n`{ca}`\n\n'
        f'Token may be too new.\n'
        f'[Check DexScreener](https://dexscreener.com/solana/{ca})'
    )
# =========================================================
# COMMANDS
# =========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        '*GemStalker* is live!\n\n'
        'Scanning pump.fun websocket in real-time.\n'
        'Paste any Solana CA for instant analysis.\n\n'
        '*Commands:*\n'
        '/status - live tracking stats\n'
        '/calls - this month calls + multiples\n'
        '/filters - active filter settings\n\n'
        'Just paste a CA to analyze it.',
        parse_mode='Markdown',
    )
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    total   = len(tokens)
    alerted = sum(1 for t in tokens.values() if t.called)
    watching = total - alerted
    candidates = sorted(
        [t for t in tokens.values() if not t.called and t.buy_count > 0],
        key=lambda t: t.sol_in, reverse=True
    )[:3]
    msg = (
        '*GemStalker Status*\n\n'
        f'Tokens tracked : {total}\n'
        f'Alerts sent    : {alerted}\n'
        f'Still watching : {watching}\n'
        f'SOL Price      : ${SOL_PRICE:.2f}\n'
    )
    if candidates:
        msg += '\n*Top candidates:*\n'
        for t in candidates:
            passed, reason = passes_filters(t)
            msg += (
                f'\n*{t.name}* ({t.symbol})\n'
                f'MC:{fmt(t.market_cap)} | Inflow:{t.sol_in:.2f}SOL\n'
                f'Blocking: {reason}\n'
            )
    await update.message.reply_text(msg, parse_mode='Markdown')
async def cmd_calls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_calls:
        await update.message.reply_text('No calls this month yet.')
        return
    now = time.time()
    month_start = time.mktime(
        time.localtime()[:2] + (1, 0, 0, 0, 0, 0, 0)
    )
    month_calls = [c for c in recent_calls if c['time'] >= month_start]
    if not month_calls:
        await update.message.reply_text('No calls this month yet.')
        return
    month_name = time.strftime('%B %Y')
    lines = [f'*Calls - {month_name}* ({len(month_calls)} total)\n']
    for i, c in enumerate(month_calls, 1):
        mint     = c['mint']
        entry_mc = c.get('entry_mc', c['market_cap'])
        mult_str = 'N/A'
        if mint in tokens:
            t = tokens[mint]
            if t.market_cap > 0 and entry_mc > 0:
                mult = t.market_cap / entry_mc
                mult_str = f'{mult:.1f}x'
        lines.append(
            f'{i}. *{c["name"]}* (${c["symbol"]})\n'
            f'   Entry: {fmt(entry_mc)} | Now: {mult_str} | Score: {c["score"]}/10'
        )
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')
async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        '*Active Filters*\n\n'
        f'MC Range     : {fmt(MC_MIN)} - {fmt(MC_MAX)}\n'
        f'Min Liq      : {fmt(MIN_LIQ_USD)}\n'
        f'Min Liq/MC   : {MIN_LIQ_RATIO:.0%}\n'
        f'Min Inflow   : {MIN_SOL_IN} SOL\n'
        f'Min Buys     : {MIN_BUYS}\n'
        f'Min B/Min    : {MIN_BUYS_PER_MIN}\n'
        f'Max Age      : {MAX_AGE_MINUTES}m\n'
        f'Max Top10    : {MAX_TOP10_PCT}%\n'
        f'Max Dev      : {MAX_DEV_PCT}%\n\n'
        f'SOL Price    : ${SOL_PRICE:.2f}',
        parse_mode='Markdown',
    )
async def handle_ca_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (update.message.text or '').strip()
    if SOLANA_CA_RE.fullmatch(text):
        ca = text
    else:
        matches = SOLANA_CA_RE.findall(text)
        if not matches:
            return
        ca = matches[0]
    msg    = await update.message.reply_text(f'Analyzing {ca[:12]}...')
    result = await analyze_ca(ca)
    try:
        await msg.edit_text(result, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception:
        await msg.edit_text(result, disable_web_page_preview=True)
# =========================================================
# WEBSOCKET EVENT HANDLER
# =========================================================
async def handle_event(app: Application, msg: dict) -> None:
    global event_counter, buy_counter
    tx_type = msg.get('txType')
    mint    = msg.get('mint')
    event_counter += 1
    if not mint:
        return
    if mint not in tokens:
        tokens[mint] = Token(mint=mint)
    t             = tokens[mint]
    t.last_active = time.time()
    t.name        = msg.get('name',   t.name)
    t.symbol      = msg.get('symbol', t.symbol)
    market_cap_sol = float(msg.get('marketCapSol', 0) or 0)
    t.market_cap   = market_cap_sol * SOL_PRICE
    virtual_sol   = float(msg.get('virtualSolReserves', 0) or 0) / 1e9
    virtual_token = float(msg.get('virtualTokenReserves', 0) or 0) / 1e6
    if virtual_sol > 0:
        t.sol_reserves   = virtual_sol
    if virtual_token > 0:
        t.token_reserves = virtual_token
    t.bonding_curve = float(msg.get('bondingCurveProgress', 0) or 0)
    sol_amount = float(msg.get('solAmount', 0) or 0)
    if tx_type == 'buy':
        buy_counter  += 1
        t.buy_count  += 1
        t.buy_volume += sol_amount
        t.sol_in     += sol_amount
        t.buys.append(time.time())
    elif tx_type == 'sell':
        t.sell_count  += 1
        t.sell_volume += sol_amount
        t.sol_out     += sol_amount
    if msg.get('raydiumPool') and not t.migrated:
        t.migrated = True
        log.info(f'Migration detected: {t.name} ({t.symbol})')
        liq = await fetch_raydium_liq(mint)
        if liq > 0:
            t.raydium_liq = liq
            log.info(f'Raydium liq fetched: {fmt(liq)} for {t.name}')
    holder_count = int(msg.get('holderCount', 0) or 0)
    if holder_count:
        t.holders = max(t.holders, holder_count)
    top_holder = float(msg.get('topHolder', 0) or 0)
    if top_holder:
        t.top_holder = top_holder
    for field_name, attr in [('twitter', 'twitter'), ('telegram', 'telegram'), ('website', 'website')]:
        val = msg.get(field_name, '')
        if val and not getattr(t, attr):
            setattr(t, attr, val)
    if t.called:
        await check_milestones(app, t)
        return
    if not liq_confirmed(t):
        log.debug(f'Liq not confirmed for {t.name}: {get_confirmed_liq(t):.2f}')
        return
    passed, reason = passes_filters(t)
    if passed:
        t.called = True
        await send_alert(app, t)
    elif tx_type == 'buy' and t.buy_count % 10 == 0:
        log.info(
            f'FAIL: {t.name} ({t.symbol}) | '
            f'MC={fmt(t.market_cap)} | Liq={fmt(get_confirmed_liq(t))} | '
            f'Inflow={t.sol_in:.2f} | Buys={t.buy_count} | Reason: {reason}'
        )
# =========================================================
# WEBSOCKET LOOP
# =========================================================
async def websocket_loop(app: Application) -> None:
    reconnect_count = 0
    while True:
        try:
            log.info(f'WS connecting (attempt {reconnect_count + 1})')
            async with websockets.connect(
                PUMP_WS,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=10,
            ) as ws:
                reconnect_count = 0
                log.info('WS connected to pumpportal.fun')
                await ws.send(json.dumps({'method': 'subscribeNewToken'}))
                await ws.send(json.dumps({'method': 'subscribeTokenTrade'}))
                log.info('WS subscribed to new tokens and trades')
                heartbeat = time.time()
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        await handle_event(app, msg)
                        if time.time() - heartbeat > 30:
                            heartbeat = time.time()
                            log.info(
                                f'WS alive | events={event_counter} tokens={len(tokens)} '
                                f'calls={sum(1 for t in tokens.values() if t.called)}'
                            )
                    except json.JSONDecodeError as e:
                        log.warning(f'Bad JSON: {e}')
                    except Exception as e:
                        log.error(f'Event error: {e}')
        except Exception as e:
            reconnect_count += 1
            wait = min(5 * reconnect_count, 30)
            log.error(f'WS disconnected: {e} - retry in {wait}s')
            await asyncio.sleep(wait)
# =========================================================
# CLEANUP
# =========================================================
async def cleanup_tokens() -> None:
    while True:
        await asyncio.sleep(300)
        cutoff = time.time() - 3600
        stale  = [m for m, t in tokens.items() if t.last_active < cutoff and not t.called]
        for m in stale:
            del tokens[m]
        if stale:
            log.info(f'Cleaned {len(stale)} stale tokens. Active: {len(tokens)}')
async def log_stats() -> None:
    global event_counter, buy_counter
    while True:
        await asyncio.sleep(60)
        log.info(
            f'STATS | Events/min: {event_counter} | Buys: {buy_counter} | '
            f'Tokens: {len(tokens)} | Alerts: {sum(1 for t in tokens.values() if t.called)}'
        )
        event_counter = 0
        buy_counter   = 0
# =========================================================
# HEALTH SERVER
# =========================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'GemStalker OK')
    def log_message(self, format, *args):
        pass
def run_health_server() -> None:
    port = int(os.getenv('PORT', 8080))
    HTTPServer(('0.0.0.0', port), HealthHandler).serve_forever()
# =========================================================
# STARTUP
# =========================================================
async def post_init(app: Application) -> None:
    log.info('Launching background tasks')
    asyncio.create_task(update_sol_price())
    asyncio.create_task(websocket_loop(app))
    asyncio.create_task(cleanup_tokens())
    asyncio.create_task(log_stats())
    log.info('All tasks running')
# =========================================================
# MAIN
# =========================================================
def main() -> None:
    if not TG_TOKEN:
        raise RuntimeError('TELEGRAM_BOT_TOKEN not set')
    if not CHAT_ID:
        raise RuntimeError('CHAT_ID not set')
    threading.Thread(target=run_health_server, daemon=True).start()
    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler('start',   cmd_start))
    app.add_handler(CommandHandler('status',  cmd_status))
    app.add_handler(CommandHandler('calls',   cmd_calls))
    app.add_handler(CommandHandler('filters', cmd_filters))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_ca_message))
    app.post_init = post_init
    log.info('GemStalker starting')
    app.run_polling(drop_pending_updates=True)
if __name__ == '__main__':
    main()
