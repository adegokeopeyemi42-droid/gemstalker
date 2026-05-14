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
# FILTERS  (relaxed for early pump.fun launches)
# =========================================================
MC_MIN           = 5_000
MC_MAX           = 200_000
MIN_LIQ_USD      = 2_000
MIN_SOL_IN       = 0.5
MIN_BUYS         = 3
MAX_AGE_MINUTES  = 45
MILESTONES       = [2, 5, 10, 25, 50, 100]
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
    mint:            str
    name:            str   = 'Unknown'
    symbol:          str   = '?'
    market_cap:      float = 0.0
    virtual_sol:     float = 0.0
    sol_in:          float = 0.0
    sol_out:         float = 0.0
    bonding_curve:   float = 0.0
    buy_count:       int   = 0
    sell_count:      int   = 0
    migrated:        bool  = False
    raydium_liq:     float = 0.0
    called:          bool  = False
    twitter:         str   = ''
    telegram_link:   str   = ''
    website:         str   = ''
    created_at:      float = field(default_factory=time.time)
    last_active:     float = field(default_factory=time.time)
    buys_ts:         deque = field(default_factory=lambda: deque(maxlen=300))
    entry_mc:        float = 0.0
    next_milestone:  int   = 2
tokens:       dict  = {}
recent_calls: deque = deque(maxlen=500)
ev_count = 0
buy_count_global = 0
# =========================================================
# LIQUIDITY FROM BONDING CURVE
# =========================================================
def bonding_curve_liq(t: Token) -> float:
    if t.virtual_sol > 0:
        return t.virtual_sol * SOL_PRICE
    if t.sol_in > 0:
        return t.sol_in * SOL_PRICE * 0.85
    return 0.0
def get_liq(t: Token) -> float:
    if t.migrated and t.raydium_liq > 0:
        return t.raydium_liq
    return bonding_curve_liq(t)
def liq_source(t: Token) -> str:
    if t.migrated and t.raydium_liq > 0:
        return 'Raydium'
    if t.virtual_sol > 0:
        return 'Bonding Curve'
    return 'Estimated'
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
                log.info(f'SOL: ${SOL_PRICE:.2f}')
        except Exception as e:
            log.debug(f'SOL price failed: {e}')
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
# FILTER
# =========================================================
def passes(t: Token) -> tuple:
    if age_min(t) > MAX_AGE_MINUTES:
        return False, f'Too old ({age_min(t):.0f}m)'
    if t.market_cap < MC_MIN:
        return False, f'MC too low ({fmt(t.market_cap)})'
    if t.market_cap > MC_MAX:
        return False, f'MC too high ({fmt(t.market_cap)})'
    liq = get_liq(t)
    if liq < MIN_LIQ_USD:
        return False, f'Liq too low ({fmt(liq)})'
    if t.sol_in < MIN_SOL_IN:
        return False, f'Inflow too low ({t.sol_in:.2f} SOL)'
    if t.buy_count < MIN_BUYS:
        return False, f'Not enough buys ({t.buy_count})'
    net = t.sol_in - t.sol_out
    if net < 0:
        return False, f'Net negative flow'
    return True, 'OK'
# =========================================================
# SECURITY READ
# =========================================================
def security_read(t: Token) -> list:
    liq = get_liq(t)
    reads = []
    p = pressure(t)
    if p >= 70:
        reads.append('Strong buy pressure')
    elif p >= 55:
        reads.append('Moderate buy pressure')
    else:
        reads.append('Sell pressure present')
    if liq >= 10_000:
        reads.append('Good liquidity')
    elif liq >= 4_000:
        reads.append('Decent liquidity')
    else:
        reads.append('Low liquidity')
    if bpm(t) >= 5:
        reads.append('High buy frequency')
    if t.migrated:
        reads.append('Migrated to Raydium')
    elif t.bonding_curve > 0:
        reads.append(f'Bonding curve {t.bonding_curve:.0f}%')
    if age_min(t) < 5:
        reads.append('Very early launch')
    elif age_min(t) < 15:
        reads.append('Early stage')
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
    net  = t.sol_in - t.sol_out
    sec  = security_read(t)
    vol  = (t.sol_in + t.sol_out) * SOL_PRICE
    socials = []
    if t.twitter:       socials.append(f'[X]({t.twitter})')
    if t.telegram_link: socials.append(f'[TG]({t.telegram_link})')
    if t.website:       socials.append(f'[Web]({t.website})')
    soc = ' | '.join(socials) if socials else 'None'
    sec_fmt = '\n'.join(f'- {s}' for s in sec)
    return (
        f'*GEM DETECTED*\n\n'
        f'*{t.name}* (${t.symbol})\n'
        f'Age: {age_str(t)}\n\n'
        f'Market Cap : {fmt(t.market_cap)}\n'
        f'Liq Pool   : {fmt(liq)} ({src})\n'
        f'Volume     : {fmt(vol)}\n'
        f'Buy / Sell : {t.buy_count} / {t.sell_count}\n'
        f'Pressure   : {p}%\n\n'
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
            r    = await c.get(f'https://api.dexscreener.com/latest/dex/tokens/{mint}')
            data = r.json()
            pairs = [p for p in (data.get('pairs') or []) if p.get('chainId') == 'solana']
            if pairs:
                best = max(pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0) or 0))
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
            r    = await c.get(f'https://frontend-api.pump.fun/coins/{mint}')
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
# =========================================================
async def send_alert(app: Application, t: Token) -> None:
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
        t.entry_mc      = t.market_cap
        t.next_milestone = 2
        recent_calls.appendleft({
            'name': t.name, 'symbol': t.symbol, 'mint': t.mint,
            'mc': t.market_cap, 'ts': time.time(),
        })
        log.info(f'ALERT: {t.name} ({t.symbol}) MC={fmt(t.market_cap)} Liq={fmt(get_liq(t))}')
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
# CA SCAN
# =========================================================
async def scan_ca(ca: str) -> str:
    if ca in tokens:
        t   = tokens[ca]
        liq = get_liq(t)
        ok, reason = passes(t)
        vol = (t.sol_in + t.sol_out) * SOL_PRICE
        socials = []
        if t.twitter:       socials.append(f'[X]({t.twitter})')
        if t.telegram_link: socials.append(f'[TG]({t.telegram_link})')
        if t.website:       socials.append(f'[Web]({t.website})')
        soc = ' | '.join(socials) if socials else 'None'
        sec = security_read(t)
        sec_fmt = '\n'.join(f'- {s}' for s in sec)
        dex    = f'https://dexscreener.com/solana/{ca}'
        photon = f'https://photon-sol.tinyastro.io/en/lp/{ca}'
        return (
            f'*{t.name}* (${t.symbol})\n'
            f'Age: {age_str(t)}\n\n'
            f'Market Cap : {fmt(t.market_cap)}\n'
            f'Liq Pool   : {fmt(liq)} ({liq_source(t)})\n'
            f'Volume     : {fmt(vol)}\n'
            f'Buy / Sell : {t.buy_count} / {t.sell_count}\n'
            f'Pressure   : {pressure(t)}%\n\n'
            f'Security\n{sec_fmt}\n\n'
            f'Socials: {soc}\n\n'
            f'Filter: {"PASS" if ok else reason}\n\n'
            f'[Dex]({dex}) | [Photon]({photon})\n'
            f'`{ca}`'
        )
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r    = await c.get(f'https://api.dexscreener.com/latest/dex/tokens/{ca}')
            data = r.json()
            pairs = [p for p in (data.get('pairs') or []) if p.get('chainId') == 'solana']
            if pairs:
                pair   = max(pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0) or 0))
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
                if pres >= 65:    reads.append('Strong buy pressure')
                elif pres >= 50:  reads.append('Moderate buy pressure')
                else:             reads.append('Sell pressure present')
                if liq >= 10_000: reads.append('Good liquidity')
                elif liq >= 3_000:reads.append('Decent liquidity')
                else:             reads.append('Low liquidity')
                if pc1h > 20:     reads.append('Strong 1h momentum')
                elif pc1h < -20:  reads.append('Dumping last hour')
                if mc < 30_000:   reads.append('Very early stage')
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
        log.warning(f'scan_ca error: {e}')
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
        'Paste any Solana CA to analyze it.\n\n'
        '/status - live stats\n'
        '/calls - this month calls\n'
        '/filters - active filters',
        parse_mode='Markdown',
    )
async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    total   = len(tokens)
    alerted = sum(1 for t in tokens.values() if t.called)
    top = sorted(
        [t for t in tokens.values() if not t.called and t.buy_count >= 2],
        key=lambda t: get_liq(t), reverse=True
    )[:3]
    msg = (
        '*GemStalker Status*\n\n'
        f'Tokens tracked : {total}\n'
        f'Alerts sent    : {alerted}\n'
        f'SOL Price      : ${SOL_PRICE:.2f}\n'
    )
    if top:
        msg += '\n*Top candidates:*\n'
        for t in top:
            ok, reason = passes(t)
            msg += (
                f'\n*{t.name}* (${t.symbol})\n'
                f'MC: {fmt(t.market_cap)} | Liq: {fmt(get_liq(t))}\n'
                f'{"PASS" if ok else reason}\n'
            )
    await update.message.reply_text(msg, parse_mode='Markdown')
async def cmd_calls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_calls:
        await update.message.reply_text('No calls this month yet.')
        return
    month_start = time.mktime(
        time.strptime(time.strftime('%Y-%m-01'), '%Y-%m-%d')
    )
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
                mult = t.market_cap / entry_mc
                mult_str = f'{mult:.1f}x'
        lines.append(
            f'{i}. *{c["name"]}* (${c["symbol"]})\n'
            f'   Entry: {fmt(entry_mc)} | Now: {mult_str}'
        )
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')
async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        '*Active Filters*\n\n'
        f'MC Range  : {fmt(MC_MIN)} - {fmt(MC_MAX)}\n'
        f'Min Liq   : {fmt(MIN_LIQ_USD)}\n'
        f'Min Inflow: {MIN_SOL_IN} SOL\n'
        f'Min Buys  : {MIN_BUYS}\n'
        f'Max Age   : {MAX_AGE_MINUTES}m\n\n'
        f'SOL Price : ${SOL_PRICE:.2f}',
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
# WEBSOCKET EVENT HANDLER
# =========================================================
async def handle_event(app: Application, msg: dict) -> None:
    global ev_count, buy_count_global
    tx_type = msg.get('txType')
    mint    = msg.get('mint')
    ev_count += 1
    if not mint:
        return
    if mint not in tokens:
        tokens[mint] = Token(mint=mint)
    t             = tokens[mint]
    t.last_active = time.time()
    t.name        = msg.get('name',   t.name)   or t.name
    t.symbol      = msg.get('symbol', t.symbol) or t.symbol
    mc_sol       = float(msg.get('marketCapSol', 0) or 0)
    t.market_cap = mc_sol * SOL_PRICE
    v_sol = float(msg.get('virtualSolReserves', 0) or 0) / 1e9
    if v_sol > 0:
        t.virtual_sol = v_sol
    t.bonding_curve = float(msg.get('bondingCurveProgress', 0) or 0)
    sol_amt = float(msg.get('solAmount', 0) or 0)
    if tx_type == 'buy':
        buy_count_global += 1
        t.buy_count += 1
        t.sol_in    += sol_amt
        t.buys_ts.append(time.time())
    elif tx_type == 'sell':
        t.sell_count += 1
        t.sol_out    += sol_amt
    for field_name, attr in [('twitter','twitter'), ('telegram','telegram_link'), ('website','website')]:
        val = msg.get(field_name, '')
        if val and not getattr(t, attr):
            setattr(t, attr, val)
    if msg.get('raydiumPool') and not t.migrated:
        t.migrated = True
        log.info(f'Migration: {t.name} ({t.symbol})')
        asyncio.create_task(fetch_and_set_raydium_liq(t))
    if t.called:
        await check_milestones(app, t)
        return
    ok, reason = passes(t)
    if ok:
        if not t.twitter and not t.telegram_link and not t.website:
            asyncio.create_task(fetch_and_set_socials(app, t))
        else:
            t.called = True
            await send_alert(app, t)
    elif tx_type == 'buy' and t.buy_count % 20 == 0:
        log.info(
            f'FAIL: {t.name} ({t.symbol}) | '
        )
            f'MC={fmt(t.market_cap)} | Liq={fmt(get_liq(t))} | '
            f'Buys={t.buy_count} | {reason}'
async def fetch_and_set_raydium_liq(t: Token) -> None:
    liq = await fetch_raydium_liq(t.mint)
    if liq > 0:
        t.raydium_liq = liq
        log.info(f'Raydium liq set: {fmt(liq)} for {t.name}')
async def fetch_and_set_socials(app: Application, t: Token) -> None:
    socials = await fetch_pump_socials(t.mint)
    if socials.get('twitter'):  t.twitter = socials['twitter']
    if socials.get('telegram'): t.telegram_link = socials['telegram']
    if socials.get('website'):  t.website = socials['website']
    t.called = True
    await send_alert(app, t)
# =========================================================
# WEBSOCKET LOOP
# =========================================================
async def websocket_loop(app: Application) -> None:
    retries = 0
    while True:
        try:
            log.info(f'WS connecting (attempt {retries+1})')
            async with websockets.connect(
                PUMP_WS,
                ping_interval=20,
                ping_timeout=30,
                close_timeout=10,
            ) as ws:
                retries = 0
                log.info('WS connected to pumpportal.fun')
                await ws.send(json.dumps({'method': 'subscribeNewToken'}))
                await ws.send(json.dumps({'method': 'subscribeTokenTrade'}))
                log.info('WS subscribed')
                hb = time.time()
                async for raw in ws:
                    try:
                        await handle_event(app, json.loads(raw))
                        if time.time() - hb > 60:
                            hb = time.time()
                            alerted = sum(1 for t in tokens.values() if t.called)
                            log.info(
                            )
                                f'WS alive | tokens={len(tokens)} '
                                f'alerts={alerted} ev={ev_count}'
                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        log.error(f'Event error: {e}')
        except Exception as e:
            retries += 1
            wait = min(5 * retries, 30)
            log.error(f'WS disconnected: {e} - retry in {wait}s')
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
        alerted = sum(1 for t in tokens.values() if t.called)
        log.info(
            f'STATS | ev={ev_count} buys={buy_count_global} '
            f'tokens={len(tokens)} alerts={alerted}'
        )
        ev_count = 0
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
    log.info('GemStalker ready')
def main() -> None:
    if not TG_TOKEN: raise RuntimeError('TELEGRAM_BOT_TOKEN not set')
    if not CHAT_ID:  raise RuntimeError('CHAT_ID not set')
    threading.Thread(target=run_health, daemon=True).start()
    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler('start',   cmd_start))
    app.add_handler(CommandHandler('status',  cmd_status))
    app.add_handler(CommandHandler('calls',   cmd_calls))
    app.add_handler(CommandHandler('filters', cmd_filters))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.post_init = post_init
    log.info('GemStalker starting')
    app.run_polling(drop_pending_updates=True)
if __name__ == '__main__':
    main()
