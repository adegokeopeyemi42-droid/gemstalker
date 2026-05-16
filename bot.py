"""
GemStalker v2 — Multi-chain post-migration gem hunter
Chains: Solana, Base, Ethereum (BSC optional)
Sources: DexScreener, Birdeye, GeckoTerminal, Rugcheck/GoPlus
"""

import os
import re
import time
import json
import asyncio
import logging
import hashlib
import threading
from dataclasses import dataclass, field
from collections import deque
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

import httpx
import websockets
from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# =========================================================
# CONFIG
# =========================================================
TG_TOKEN      = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID       = os.getenv('CHAT_ID')
BIRDEYE_KEY   = os.getenv('BIRDEYE_API_KEY', '')
GOPLUS_KEY    = os.getenv('GOPLUS_API_KEY', '')
PUMPPORTAL_KEY = os.getenv('PUMPPORTAL_API_KEY', '')

# Polling intervals (seconds)
DEXSCREENER_POLL_INTERVAL = 30    # poll trending / new pairs
BIRDEYE_POLL_INTERVAL     = 45
CLEANUP_INTERVAL          = 600
STATS_INTERVAL            = 120

# Alert cooldowns
ALERT_COOLDOWN_SEC  = 86400        # 24h no-repeat per token
ALERT_DELAY_SEC     = 5            # re-validate before sending

# =========================================================
# SUPPORTED CHAINS
# =========================================================
CHAINS = {
    'solana':   {'id': 'solana',   'name': 'Solana',   'emoji': '◎'},
    'base':     {'id': 'base',     'name': 'Base',     'emoji': '🔵'},
    'ethereum': {'id': 'ethereum', 'name': 'Ethereum', 'emoji': '⟠'},
    # 'bsc':    {'id': 'bsc',      'name': 'BSC',      'emoji': '🟡'},
}

# =========================================================
# CORE FILTERS
# =========================================================
MC_MIN             = 40_000        # $40K minimum market cap
MC_MAX             = 2_000_000     # $2M maximum (already extended)
LIQ_MC_RATIO_MIN   = 0.15         # liquidity must be ≥15% of MC
VOL_1H_MIN         = 20_000       # $20K minimum 1h volume
VOL_5M_MIN         = 2_000        # $2K minimum 5m volume spike
BUY_PRESSURE_MIN   = 55           # buy% must exceed sells
HOLDER_MIN         = 100          # minimum holder count
HOLDER_GROWTH_MIN  = 5            # new holders in last scan cycle
MAX_TOP_HOLDER_PCT = 20.0         # max % held by single wallet
MIN_SCORE          = 65           # minimum score to alert
REQUIRE_SOCIALS    = True         # must have at least one social
MIGRATION_ONLY     = True         # only recently migrated / launched tokens
MIGRATION_MAX_HOURS = 72          # max hours since migration/launch

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
class GemToken:
    address:       str
    chain:         str         = 'solana'
    name:          str         = 'Unknown'
    symbol:        str         = '?'
    market_cap:    float       = 0.0
    liquidity_usd: float       = 0.0
    vol_5m:        float       = 0.0
    vol_1h:        float       = 0.0
    vol_6h:        float       = 0.0
    vol_24h:       float       = 0.0
    buys_5m:       int         = 0
    sells_5m:      int         = 0
    buys_1h:       int         = 0
    sells_1h:      int         = 0
    price_usd:     float       = 0.0
    price_change_5m:  float    = 0.0
    price_change_1h:  float    = 0.0
    price_change_6h:  float    = 0.0
    holder_count:  int         = 0
    holder_count_prev: int     = 0
    top_holder_pct: float      = 0.0
    has_mint_auth: bool        = False
    has_freeze_auth: bool      = False
    is_honeypot:   bool        = False
    lp_locked:     bool        = False
    lp_lock_pct:   float       = 0.0
    rug_score:     int         = 0       # 0=safe, 100=rugged
    twitter:       str         = ''
    telegram_link: str         = ''
    website:       str         = ''
    dex_url:       str         = ''
    pair_address:  str         = ''
    dex_id:        str         = ''
    created_at:    float       = field(default_factory=time.time)
    migrated_at:   float       = field(default_factory=time.time)
    last_updated:  float       = field(default_factory=time.time)
    last_scanned:  float       = 0.0
    score:         int         = 0
    score_reasons: list        = field(default_factory=list)
    caution_label: str         = ''
    heat_label:    str         = ''
    called:        bool        = False
    called_at:     float       = 0.0
    entry_mc:      float       = 0.0
    peak_mult:     float       = 1.0
    next_milestone: int        = 2
    vol_history:   deque       = field(default_factory=lambda: deque(maxlen=10))
    holder_history: deque      = field(default_factory=lambda: deque(maxlen=10))

MILESTONES = [2, 3, 5, 10, 25, 50, 100]

# Global state
gems:          dict  = {}          # address -> GemToken
alerted_set:   set   = set()       # addresses already alerted (24h cooldown)
recent_calls:  deque = deque(maxlen=500)
scan_count:    int   = 0

# =========================================================
# SCORING ENGINE
# =========================================================
def compute_score(t: GemToken) -> tuple[int, list[str]]:
    score   = 0
    reasons = []

    # 1. Volume growth (+30 max)
    if t.vol_1h >= 200_000:
        score += 30; reasons.append(f'+30 vol_1h=${t.vol_1h/1000:.0f}K')
    elif t.vol_1h >= 100_000:
        score += 22; reasons.append(f'+22 vol_1h=${t.vol_1h/1000:.0f}K')
    elif t.vol_1h >= 50_000:
        score += 15; reasons.append(f'+15 vol_1h=${t.vol_1h/1000:.0f}K')
    elif t.vol_1h >= 20_000:
        score += 8;  reasons.append(f'+8 vol_1h=${t.vol_1h/1000:.0f}K')

    # 5m spike bonus
    if t.vol_5m >= 10_000:
        score += 10; reasons.append(f'+10 vol_5m spike=${t.vol_5m/1000:.1f}K')
    elif t.vol_5m >= 5_000:
        score += 5;  reasons.append(f'+5 vol_5m=${t.vol_5m/1000:.1f}K')

    # 2. Liquidity quality (+20 max)
    liq_ratio = t.liquidity_usd / t.market_cap if t.market_cap > 0 else 0
    if liq_ratio >= 0.40:
        score += 20; reasons.append(f'+20 liq_ratio={liq_ratio:.0%}')
    elif liq_ratio >= 0.25:
        score += 14; reasons.append(f'+14 liq_ratio={liq_ratio:.0%}')
    elif liq_ratio >= 0.15:
        score += 8;  reasons.append(f'+8 liq_ratio={liq_ratio:.0%}')

    # 3. Buy pressure (+15 max)
    total_5m = t.buys_5m + t.sells_5m
    total_1h = t.buys_1h + t.sells_1h
    bp_5m = t.buys_5m / total_5m * 100 if total_5m else 0
    bp_1h = t.buys_1h / total_1h * 100 if total_1h else 0
    avg_bp = (bp_5m + bp_1h) / 2
    if avg_bp >= 75:
        score += 15; reasons.append(f'+15 buy_pressure={avg_bp:.0f}%')
    elif avg_bp >= 65:
        score += 10; reasons.append(f'+10 buy_pressure={avg_bp:.0f}%')
    elif avg_bp >= 55:
        score += 5;  reasons.append(f'+5 buy_pressure={avg_bp:.0f}%')

    # 4. Holder growth (+15 max)
    holder_growth = t.holder_count - t.holder_count_prev
    if t.holder_count >= 2000:
        score += 10; reasons.append(f'+10 holders={t.holder_count}')
    elif t.holder_count >= 500:
        score += 6;  reasons.append(f'+6 holders={t.holder_count}')
    elif t.holder_count >= 100:
        score += 3;  reasons.append(f'+3 holders={t.holder_count}')
    if holder_growth >= 50:
        score += 5;  reasons.append(f'+5 holder_growth=+{holder_growth}')
    elif holder_growth >= 15:
        score += 3;  reasons.append(f'+3 holder_growth=+{holder_growth}')

    # 5. Socials (+10 max)
    soc_count = sum([bool(t.twitter), bool(t.telegram_link), bool(t.website)])
    if soc_count >= 3:
        score += 10; reasons.append('+10 all_socials')
    elif soc_count >= 2:
        score += 7;  reasons.append('+7 two_socials')
    elif soc_count >= 1:
        score += 4;  reasons.append('+4 one_social')

    # 6. Momentum / price action (+10 max)
    if t.price_change_1h >= 50:
        score += 10; reasons.append(f'+10 price_1h=+{t.price_change_1h:.0f}%')
    elif t.price_change_1h >= 25:
        score += 6;  reasons.append(f'+6 price_1h=+{t.price_change_1h:.0f}%')
    elif t.price_change_1h >= 10:
        score += 3;  reasons.append(f'+3 price_1h=+{t.price_change_1h:.0f}%')

    # 7. Security bonuses
    if t.lp_locked and t.lp_lock_pct >= 80:
        score += 5;  reasons.append(f'+5 LP_locked={t.lp_lock_pct:.0f}%')
    elif t.lp_locked:
        score += 2;  reasons.append('+2 LP_locked')

    # --- PENALTIES ---
    if t.has_mint_auth:
        score -= 20; reasons.append('-20 mint_authority_enabled')
    if t.has_freeze_auth:
        score -= 15; reasons.append('-15 freeze_authority_enabled')
    if t.is_honeypot:
        score -= 50; reasons.append('-50 honeypot_detected')
    if t.rug_score >= 70:
        score -= 30; reasons.append(f'-30 rug_score={t.rug_score}')
    elif t.rug_score >= 40:
        score -= 10; reasons.append(f'-10 rug_score={t.rug_score}')
    if t.top_holder_pct >= 30:
        score -= 20; reasons.append(f'-20 top_holder={t.top_holder_pct:.0f}%')
    elif t.top_holder_pct >= 20:
        score -= 10; reasons.append(f'-10 top_holder={t.top_holder_pct:.0f}%')

    # Wash trading proxy: 0 holders but massive volume
    if t.vol_1h > 500_000 and t.holder_count < 50:
        score -= 25; reasons.append('-25 suspected_wash_trading')

    # Sell pressure penalty
    if avg_bp < 40:
        score -= 15; reasons.append(f'-15 heavy_sell_pressure={avg_bp:.0f}%')

    return max(score, 0), reasons


def passes_hard_filters(t: GemToken) -> tuple[bool, str]:
    if t.market_cap < MC_MIN:
        return False, f'MC too low ({fmt(t.market_cap)})'
    if t.market_cap > MC_MAX:
        return False, f'MC too high ({fmt(t.market_cap)})'
    if t.liquidity_usd <= 0:
        return False, 'No liquidity data'
    liq_ratio = t.liquidity_usd / t.market_cap
    if liq_ratio < LIQ_MC_RATIO_MIN:
        return False, f'Liq/MC ratio too low ({liq_ratio:.0%})'
    if t.vol_1h < VOL_1H_MIN:
        return False, f'Vol 1h too low ({fmt(t.vol_1h)})'
    if t.vol_5m < VOL_5M_MIN:
        return False, f'Vol 5m too low ({fmt(t.vol_5m)})'
    total_1h = t.buys_1h + t.sells_1h
    bp_1h = t.buys_1h / total_1h * 100 if total_1h else 0
    if bp_1h < BUY_PRESSURE_MIN:
        return False, f'Buy pressure too low ({bp_1h:.0f}%)'
    if t.holder_count < HOLDER_MIN:
        return False, f'Holders too low ({t.holder_count})'
    if REQUIRE_SOCIALS and not any([t.twitter, t.telegram_link, t.website]):
        return False, 'No socials'
    if t.is_honeypot:
        return False, 'Honeypot detected'
    if t.has_mint_auth:
        return False, 'Mint authority active'
    age_hours = (time.time() - t.migrated_at) / 3600
    if MIGRATION_ONLY and age_hours > MIGRATION_MAX_HOURS:
        return False, f'Too old ({age_hours:.0f}h since migration)'
    return True, 'OK'


def get_caution_label(t: GemToken) -> str:
    if t.is_honeypot or t.has_mint_auth:
        return '🔴 HIGH RISK'
    if t.rug_score >= 50:
        return '🟠 POSSIBLE BOT ACTIVITY'
    liq_ratio = t.liquidity_usd / t.market_cap if t.market_cap else 0
    total_1h  = t.buys_1h + t.sells_1h
    bp_1h     = t.buys_1h / total_1h * 100 if total_1h else 0
    if t.price_change_1h >= 50 and bp_1h >= 70 and liq_ratio >= 0.25:
        return '🟢 SAFE MOMENTUM'
    if t.price_change_1h >= 80:
        return '🔵 EARLY BREAKOUT'
    if t.vol_1h >= 100_000 and bp_1h >= 65:
        return '⚡ TRENDING FAST'
    if t.score >= 80:
        return '💎 HIGH CONVICTION'
    return '🟡 HIGH RISK HIGH REWARD'


def get_heat_label(t: GemToken) -> str:
    if t.vol_5m >= 15_000 and t.price_change_5m >= 10:
        return '🔥 EXPLODING'
    if t.vol_5m >= 5_000 and t.price_change_5m >= 5:
        return '♨️ WARMING'
    if t.vol_1h >= 50_000:
        return '🌡️ HEATING'
    return '❄️ COLD'


def potential_rank(t: GemToken) -> str:
    if t.score >= 90:
        return '🚀 POTENTIAL 10x+'
    if t.score >= 75:
        return '📈 POTENTIAL 5x'
    if t.score >= 65:
        return '📊 POTENTIAL 2-3x'
    return '🎯 SPECULATIVE'


# =========================================================
# FORMATTING HELPERS
# =========================================================
def fmt(n, dec=1) -> str:
    if n is None: return 'N/A'
    n = float(n)
    if n >= 1_000_000: return f'${n/1_000_000:.{dec}f}M'
    if n >= 1_000:     return f'${n/1_000:.{dec}f}K'
    return f'${n:.{dec}f}'

def fmt_pct(n) -> str:
    if n >= 0: return f'+{n:.1f}%'
    return f'{n:.1f}%'

def chain_emoji(chain: str) -> str:
    return CHAINS.get(chain, {}).get('emoji', '🔗')

def chain_name(chain: str) -> str:
    return CHAINS.get(chain, {}).get('name', chain.capitalize())

def age_str(t: GemToken) -> str:
    s = int(time.time() - t.migrated_at)
    if s < 60:   return f'{s}s'
    if s < 3600: return f'{s//60}m'
    return f'{s//3600}h{(s%3600)//60}m'


# =========================================================
# ALERT BUILDER
# =========================================================
def build_alert(t: GemToken) -> str:
    total_1h = t.buys_1h + t.sells_1h
    bp_1h    = t.buys_1h / total_1h * 100 if total_1h else 0
    liq_ratio = t.liquidity_usd / t.market_cap * 100 if t.market_cap else 0
    holder_delta = t.holder_count - t.holder_count_prev

    socials = []
    if t.twitter:       socials.append(f'[Twitter/X]({t.twitter})')
    if t.telegram_link: socials.append(f'[Telegram]({t.telegram_link})')
    if t.website:       socials.append(f'[Website]({t.website})')
    soc_str = ' · '.join(socials) if socials else 'None'

    # Why this alert fired
    top_reasons = [r for r in t.score_reasons if r.startswith('+')][:5]
    reasons_str = '\n'.join(f'  · {r.split(" ", 1)[1]}' for r in top_reasons)

    # Security flags
    sec_flags = []
    if t.lp_locked:     sec_flags.append(f'LP Locked {t.lp_lock_pct:.0f}%')
    if not t.has_mint_auth:   sec_flags.append('No Mint Auth ✓')
    if not t.has_freeze_auth: sec_flags.append('No Freeze Auth ✓')
    if t.rug_score > 0: sec_flags.append(f'Rug Score: {t.rug_score}/100')
    sec_str = ' · '.join(sec_flags) if sec_flags else 'Not verified'

    holder_str = f'{t.holder_count:,}'
    if holder_delta > 0:
        holder_str += f' (+{holder_delta} recent)'

    dex_url    = t.dex_url or f'https://dexscreener.com/{t.chain}/{t.address}'
    photon_url = f'https://photon-sol.tinyastro.io/en/lp/{t.address}' if t.chain == 'solana' else ''
    bullx_url  = f'https://bullx.io/terminal?chainId=1399811149&address={t.address}' if t.chain == 'solana' else ''

    lines = [
        f'🚨 *MIGRATION GEM DETECTED* 🚨',
        f'',
        f'{chain_emoji(t.chain)} *{t.name}* (${t.symbol}) · {chain_name(t.chain)}',
        f'',
        f'💰 MC: {fmt(t.market_cap)}  ·  Age: {age_str(t)}',
        f'💧 Liquidity: {fmt(t.liquidity_usd)} ({liq_ratio:.0f}% of MC)',
        f'📈 Volume: 5m {fmt(t.vol_5m)} · 1h {fmt(t.vol_1h)} · 24h {fmt(t.vol_24h)}',
        f'🔥 Buy Pressure: {bp_1h:.0f}% ({t.buys_1h}B / {t.sells_1h}S last 1h)',
        f'📊 Price Change: {fmt_pct(t.price_change_5m)} 5m · {fmt_pct(t.price_change_1h)} 1h · {fmt_pct(t.price_change_6h)} 6h',
        f'👥 Holders: {holder_str}',
        f'',
        f'🔐 Security: {sec_str}',
        f'',
        f'🌐 Socials: {soc_str}',
        f'',
        f'🎯 Score: *{t.score}/100*  ·  {potential_rank(t)}',
        f'{t.caution_label}  ·  {t.heat_label}',
        f'',
        f'⚡ *Why this alert:*',
        f'{reasons_str}',
        f'',
        f'`{t.address}`',
    ]
    return '\n'.join(lines)


# =========================================================
# DEXSCREENER API
# =========================================================
async def fetch_dexscreener_token(address: str, chain: str) -> Optional[dict]:
    """Fetch token pair data from DexScreener."""
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(f'https://api.dexscreener.com/latest/dex/tokens/{address}')
            data = r.json()
            pairs = [p for p in (data.get('pairs') or [])
                     if p.get('chainId') == chain]
            if not pairs:
                return None
            # Best pair by liquidity
            best = max(pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0)))
            return best
    except Exception as e:
        log.debug(f'DexScreener fetch error for {address}: {e}')
        return None


async def fetch_dexscreener_trending(chain: str) -> list[dict]:
    """Fetch recently boosted / trending pairs from DexScreener."""
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(
                f'https://api.dexscreener.com/latest/dex/search',
                params={'q': chain},
            )
            pairs = r.json().get('pairs') or []
            # Filter to the requested chain
            return [p for p in pairs if p.get('chainId') == chain]
    except Exception as e:
        log.debug(f'DexScreener trending error: {e}')
        return []


async def fetch_dexscreener_new_pairs(chain: str) -> list[dict]:
    """Fetch newly listed pairs from DexScreener token profiles / boosts endpoint."""
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get('https://api.dexscreener.com/token-boosts/latest/v1')
            boosts = r.json()
            if isinstance(boosts, list):
                return [b for b in boosts if b.get('chainId') == chain]
            return []
    except Exception as e:
        log.debug(f'DexScreener new pairs error: {e}')
        return []


def parse_dex_pair(pair: dict, chain: str) -> Optional[GemToken]:
    """Convert a DexScreener pair object into a GemToken."""
    try:
        base     = pair.get('baseToken', {})
        address  = base.get('address', '')
        if not address:
            return None

        liq      = float((pair.get('liquidity') or {}).get('usd', 0) or 0)
        mc       = float(pair.get('fdv') or pair.get('marketCap') or 0)
        if mc == 0 and liq > 0:
            mc = liq * 3    # rough estimate

        txns     = pair.get('txns') or {}
        vol      = pair.get('volume') or {}
        pc       = pair.get('priceChange') or {}
        info     = pair.get('info') or {}
        socials_raw = info.get('socials') or []
        websites_raw = info.get('websites') or []

        buys_5m  = int((txns.get('m5') or {}).get('buys', 0))
        sells_5m = int((txns.get('m5') or {}).get('sells', 0))
        buys_1h  = int((txns.get('h1') or {}).get('buys', 0))
        sells_1h = int((txns.get('h1') or {}).get('sells', 0))

        twitter  = next((s.get('url','') for s in socials_raw if s.get('type','').lower() in ('twitter','x')), '')
        tg       = next((s.get('url','') for s in socials_raw if s.get('type','').lower() == 'telegram'), '')
        website  = next((w.get('url','') for w in websites_raw), '')

        created_ts = pair.get('pairCreatedAt')
        created_ts = created_ts / 1000 if created_ts and created_ts > 1e12 else (created_ts or time.time())

        t = GemToken(
            address       = address,
            chain         = chain,
            name          = base.get('name', 'Unknown')[:80],
            symbol        = base.get('symbol', '?')[:20],
            market_cap    = mc,
            liquidity_usd = liq,
            vol_5m        = float(vol.get('m5', 0) or 0),
            vol_1h        = float(vol.get('h1', 0) or 0),
            vol_6h        = float(vol.get('h6', 0) or 0),
            vol_24h       = float(vol.get('h24', 0) or 0),
            buys_5m       = buys_5m,
            sells_5m      = sells_5m,
            buys_1h       = buys_1h,
            sells_1h      = sells_1h,
            price_usd     = float(pair.get('priceUsd', 0) or 0),
            price_change_5m  = float(pc.get('m5', 0) or 0),
            price_change_1h  = float(pc.get('h1', 0) or 0),
            price_change_6h  = float(pc.get('h6', 0) or 0),
            twitter       = twitter,
            telegram_link = tg,
            website       = website,
            dex_url       = f'https://dexscreener.com/{chain}/{pair.get("pairAddress","")}',
            pair_address  = pair.get('pairAddress', ''),
            dex_id        = pair.get('dexId', ''),
            migrated_at   = created_ts,
            created_at    = created_ts,
        )
        return t
    except Exception as e:
        log.debug(f'parse_dex_pair error: {e}')
        return None


# =========================================================
# BIRDEYE API (Solana)
# =========================================================
async def fetch_birdeye_token(address: str) -> Optional[dict]:
    if not BIRDEYE_KEY:
        return None
    try:
        headers = {'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana'}
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                f'https://public-api.birdeye.so/defi/token_overview',
                headers=headers,
                params={'address': address},
            )
            data = r.json()
            return data.get('data')
    except Exception as e:
        log.debug(f'Birdeye fetch error: {e}')
        return None


async def fetch_birdeye_trending() -> list[dict]:
    """Get trending tokens from Birdeye."""
    if not BIRDEYE_KEY:
        return []
    try:
        headers = {'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana'}
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(
                'https://public-api.birdeye.so/defi/trending_tokens',
                headers=headers,
                params={'sort_by': 'volume1hUSD', 'sort_type': 'desc', 'limit': 50},
            )
            return r.json().get('data', {}).get('tokens', [])
    except Exception as e:
        log.debug(f'Birdeye trending error: {e}')
        return []


async def fetch_birdeye_new_listings() -> list[dict]:
    """Get newest token listings from Birdeye."""
    if not BIRDEYE_KEY:
        return []
    try:
        headers = {'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana'}
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(
                'https://public-api.birdeye.so/defi/new_listing',
                headers=headers,
                params={'limit': 50, 'time_to': int(time.time()), 'time_from': int(time.time()) - 3600},
            )
            return r.json().get('data', {}).get('items', [])
    except Exception as e:
        log.debug(f'Birdeye new listings error: {e}')
        return []


async def enrich_from_birdeye(t: GemToken) -> None:
    """Augment token with Birdeye holder data (Solana only)."""
    if t.chain != 'solana' or not BIRDEYE_KEY:
        return
    data = await fetch_birdeye_token(t.address)
    if not data:
        return
    t.holder_count = int(data.get('holder', t.holder_count) or t.holder_count)
    # Birdeye also returns trade24h, etc.
    if data.get('trade24h'):
        pass  # could enrich further


# =========================================================
# RUGCHECK / GOPLUS SECURITY
# =========================================================
async def fetch_rugcheck(address: str) -> dict:
    """Fetch security data from Rugcheck (Solana)."""
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f'https://api.rugcheck.xyz/v1/tokens/{address}/report/summary')
            data = r.json()
            score = data.get('score', 0)
            risks = data.get('risks', [])
            has_mint   = any('mint' in str(r).lower() for r in risks)
            has_freeze = any('freeze' in str(r).lower() for r in risks)
            return {
                'rug_score':      score,
                'has_mint_auth':  has_mint,
                'has_freeze_auth':has_freeze,
            }
    except Exception as e:
        log.debug(f'Rugcheck error for {address}: {e}')
        return {}


async def fetch_goplus(address: str, chain: str) -> dict:
    """Fetch security from GoPlus Security API."""
    chain_map = {'ethereum': '1', 'bsc': '56', 'base': '8453', 'solana': 'solana'}
    chain_id  = chain_map.get(chain, '1')
    base_url  = ('https://api.gopluslabs.io/api/v1/token_security/'
                 if chain != 'solana' else
                 'https://api.gopluslabs.io/api/v1/solana/token_security/')
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            params = {'contract_addresses': address}
            if GOPLUS_KEY:
                params['access_token'] = GOPLUS_KEY
            if chain != 'solana':
                url = f'{base_url}{chain_id}'
            else:
                url = base_url
            r    = await c.get(url, params=params)
            data = r.json().get('result', {}).get(address.lower(), {})
            if not data:
                return {}
            return {
                'is_honeypot':    data.get('is_honeypot') == '1',
                'has_mint_auth':  data.get('can_take_back_ownership') == '1' or data.get('mintable') == '1',
                'has_freeze_auth':data.get('transfer_pausable') == '1',
                'lp_locked':      data.get('lp_locked_percent', '') != '' and float(data.get('lp_locked_percent', 0) or 0) > 0,
                'lp_lock_pct':    float(data.get('lp_locked_percent', 0) or 0),
                'top_holder_pct': float(data.get('top10_holder_rate', 0) or 0) * 100,
                'holder_count':   int(data.get('holder_count', 0) or 0),
            }
    except Exception as e:
        log.debug(f'GoPlus error for {address}: {e}')
        return {}


async def run_security_checks(t: GemToken) -> None:
    """Run all security checks and update the token."""
    tasks = []
    if t.chain == 'solana':
        tasks.append(fetch_rugcheck(t.address))
    tasks.append(fetch_goplus(t.address, t.chain))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, dict):
            for key, val in result.items():
                if hasattr(t, key) and val is not None:
                    # Don't downgrade lp_lock if already set
                    if key == 'lp_locked' and t.lp_locked:
                        continue
                    setattr(t, key, val)


# =========================================================
# GECKOTERM INAL (for EVM chains)
# =========================================================
async def fetch_gecko_new_pools(chain: str) -> list[dict]:
    """Fetch new pools from GeckoTerminal."""
    gecko_chains = {'ethereum': 'eth', 'base': 'base', 'solana': 'solana', 'bsc': 'bsc'}
    g_chain = gecko_chains.get(chain, chain)
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(
                f'https://api.geckoterminal.com/api/v2/networks/{g_chain}/new_pools',
                params={'page': 1},
                headers={'Accept': 'application/json;version=20230302'},
            )
            data   = r.json()
            pools  = data.get('data', [])
            return pools
    except Exception as e:
        log.debug(f'GeckoTerminal error: {e}')
        return []


async def gecko_pool_to_token(pool: dict, chain: str) -> Optional[GemToken]:
    """Parse a GeckoTerminal pool into a GemToken stub for further enrichment."""
    try:
        attrs    = pool.get('attributes', {})
        rels     = pool.get('relationships', {})
        base_tok = rels.get('base_token', {}).get('data', {})
        token_id = base_tok.get('id', '')  # e.g. "eth_0x..."
        address  = token_id.split('_', 1)[-1] if '_' in token_id else token_id
        if not address:
            return None

        mc       = float(attrs.get('fully_diluted_valuation') or 0)
        liq      = float(attrs.get('reserve_in_usd') or 0)
        vol_24h  = float(attrs.get('volume_usd', {}).get('h24', 0) or 0)
        vol_1h   = float(attrs.get('volume_usd', {}).get('h1', 0) or 0)
        created  = attrs.get('pool_created_at')
        created_ts = time.time()
        if created:
            from datetime import datetime, timezone
            try:
                created_ts = datetime.fromisoformat(created.replace('Z', '+00:00')).timestamp()
            except Exception:
                pass

        name   = attrs.get('name', 'Unknown').split('/')[0].strip()
        symbol = name[:10]

        t = GemToken(
            address       = address,
            chain         = chain,
            name          = name,
            symbol        = symbol,
            market_cap    = mc,
            liquidity_usd = liq,
            vol_1h        = vol_1h,
            vol_24h       = vol_24h,
            migrated_at   = created_ts,
            created_at    = created_ts,
            dex_url       = f'https://dexscreener.com/{chain}/{address}',
        )
        return t
    except Exception as e:
        log.debug(f'gecko_pool_to_token error: {e}')
        return None


# =========================================================
# CORE EVALUATION PIPELINE
# =========================================================
async def evaluate_and_alert(app: Application, t: GemToken) -> None:
    """Full pipeline: security → score → filter → alert."""
    # Security checks first
    await run_security_checks(t)

    # Enrich with Birdeye if Solana
    if t.chain == 'solana':
        await enrich_from_birdeye(t)

    # Score
    score, reasons = compute_score(t)
    t.score         = score
    t.score_reasons = reasons
    t.caution_label = get_caution_label(t)
    t.heat_label    = get_heat_label(t)
    t.last_updated  = time.time()

    # Hard filters
    ok, reason = passes_hard_filters(t)
    if not ok:
        log.debug(f'SKIP [{t.chain}] {t.name} ({t.symbol}): {reason}')
        return

    # Score threshold
    if score < MIN_SCORE:
        log.debug(f'SKIP [{t.chain}] {t.name}: score {score} < {MIN_SCORE}')
        return

    # Duplicate check
    if t.address in alerted_set:
        log.debug(f'DUPLICATE skip: {t.address}')
        return

    # Store in gems dict
    gems[t.address] = t

    # Delay + re-validate
    await asyncio.sleep(ALERT_DELAY_SEC)
    ok, reason = passes_hard_filters(t)
    if not ok:
        log.info(f'ALERT BLOCKED (post-delay): {t.name} | {reason}')
        return
    score, reasons = compute_score(t)
    if score < MIN_SCORE:
        log.info(f'ALERT BLOCKED (post-delay score): {t.name} | {score}')
        return

    await send_alert(app, t)


async def send_alert(app: Application, t: GemToken) -> None:
    dex_url    = t.dex_url or f'https://dexscreener.com/{t.chain}/{t.address}'
    buttons    = [[InlineKeyboardButton('📊 DexScreener', url=dex_url)]]
    if t.chain == 'solana':
        photon = f'https://photon-sol.tinyastro.io/en/lp/{t.address}'
        bullx  = f'https://bullx.io/terminal?chainId=1399811149&address={t.address}'
        buttons[0].append(InlineKeyboardButton('⚡ Photon', url=photon))
        buttons[0].append(InlineKeyboardButton('🐂 BullX',  url=bullx))
    if t.twitter:
        buttons.append([InlineKeyboardButton('🐦 Twitter', url=t.twitter)])
    if t.telegram_link:
        buttons[-1].append(InlineKeyboardButton('📢 Telegram', url=t.telegram_link))

    kb = InlineKeyboardMarkup(buttons)
    try:
        await app.bot.send_message(
            chat_id    = CHAT_ID,
            text       = build_alert(t),
            parse_mode = 'Markdown',
            disable_web_page_preview = True,
            reply_markup = kb,
        )
        t.called    = True
        t.called_at = time.time()
        t.entry_mc  = t.market_cap
        t.next_milestone = 2
        alerted_set.add(t.address)
        recent_calls.appendleft({
            'name': t.name, 'symbol': t.symbol, 'address': t.address,
            'chain': t.chain, 'mc': t.market_cap, 'score': t.score, 'ts': time.time(),
        })
        log.info(
            f'✅ ALERT: [{t.chain}] {t.name} ({t.symbol}) | '
            f'MC={fmt(t.market_cap)} | Liq={fmt(t.liquidity_usd)} | '
            f'Vol1h={fmt(t.vol_1h)} | Score={t.score} | {t.caution_label}'
        )
    except Exception as e:
        log.error(f'Alert send error: {e}')


# =========================================================
# MILESTONE TRACKER
# =========================================================
async def check_milestones(app: Application, t: GemToken) -> None:
    if not t.called or not t.entry_mc or not t.next_milestone:
        return
    if t.market_cap <= 0:
        return
    mult = t.market_cap / t.entry_mc
    t.peak_mult = max(t.peak_mult, mult)
    if mult >= t.next_milestone:
        m      = t.next_milestone
        nxt    = next((x for x in MILESTONES if x > m), None)
        t.next_milestone = nxt
        try:
            await app.bot.send_message(
                chat_id    = CHAT_ID,
                parse_mode = 'Markdown',
                text       = (
                    f'🏆 *{m}x MILESTONE HIT*\n\n'
                    f'{chain_emoji(t.chain)} *{t.name}* (${t.symbol})\n'
                    f'Entry : {fmt(t.entry_mc)}\n'
                    f'Now   : {fmt(t.market_cap)}\n'
                    f'*{mult:.1f}x* from call  ·  Peak: {t.peak_mult:.1f}x\n\n'
                    f'`{t.address}`'
                ),
            )
        except Exception as e:
            log.error(f'Milestone error: {e}')


# =========================================================
# SCAN LOOPS
# =========================================================
async def scan_dexscreener(app: Application) -> None:
    """Main DexScreener polling loop — scans all supported chains."""
    while True:
        global scan_count
        scan_count += 1
        tasks = []

        for chain in CHAINS:
            tasks.append(_scan_dex_chain(app, chain))

        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(DEXSCREENER_POLL_INTERVAL)


async def _scan_dex_chain(app: Application, chain: str) -> None:
    try:
        # 1. Boosted / new listings
        boosts = await fetch_dexscreener_new_pairs(chain)
        for boost in boosts[:30]:
            address = boost.get('tokenAddress')
            if not address or address in alerted_set:
                continue
            pair = await fetch_dexscreener_token(address, chain)
            if not pair:
                continue
            t = parse_dex_pair(pair, chain)
            if t:
                existing = gems.get(t.address)
                if existing and existing.called:
                    # Update MC for milestone tracking
                    existing.market_cap = t.market_cap
                    await check_milestones(app, existing)
                elif t.address not in alerted_set:
                    asyncio.create_task(evaluate_and_alert(app, t))

        # 2. Search for fresh pairs on each chain
        pairs = await fetch_dexscreener_trending(chain)
        _process_pairs(app, pairs, chain)

    except Exception as e:
        log.error(f'DexScreener scan error [{chain}]: {e}')


def _process_pairs(app: Application, pairs: list, chain: str) -> None:
    for pair in pairs[:50]:
        if pair.get('chainId') != chain:
            continue
        t = parse_dex_pair(pair, chain)
        if not t:
            continue
        existing = gems.get(t.address)
        if existing and existing.called:
            existing.market_cap = t.market_cap
            asyncio.create_task(check_milestones(app, existing))
        elif t.address not in alerted_set:
            asyncio.create_task(evaluate_and_alert(app, t))


async def scan_birdeye(app: Application) -> None:
    """Birdeye scanning loop (Solana-only)."""
    while True:
        try:
            # Trending
            tokens_list = await fetch_birdeye_trending()
            for tok in tokens_list:
                address = tok.get('address')
                if not address or address in alerted_set:
                    continue
                pair = await fetch_dexscreener_token(address, 'solana')
                if not pair:
                    continue
                t = parse_dex_pair(pair, 'solana')
                if t and t.address not in alerted_set:
                    asyncio.create_task(evaluate_and_alert(app, t))

            # New listings
            new_listings = await fetch_birdeye_new_listings()
            for listing in new_listings:
                address = listing.get('address')
                if not address or address in alerted_set:
                    continue
                pair = await fetch_dexscreener_token(address, 'solana')
                if not pair:
                    continue
                t = parse_dex_pair(pair, 'solana')
                if t and t.address not in alerted_set:
                    asyncio.create_task(evaluate_and_alert(app, t))

        except Exception as e:
            log.error(f'Birdeye scan error: {e}')

        await asyncio.sleep(BIRDEYE_POLL_INTERVAL)


async def scan_gecko_evm(app: Application) -> None:
    """GeckoTerminal scan for EVM chains (Base, Ethereum)."""
    while True:
        for chain in ('base', 'ethereum'):
            try:
                pools = await fetch_gecko_new_pools(chain)
                for pool in pools:
                    t = await gecko_pool_to_token(pool, chain)
                    if not t or t.address in alerted_set:
                        continue
                    # Enrich with DexScreener for full data
                    pair = await fetch_dexscreener_token(t.address, chain)
                    if pair:
                        t2 = parse_dex_pair(pair, chain)
                        if t2:
                            t = t2
                    asyncio.create_task(evaluate_and_alert(app, t))
            except Exception as e:
                log.error(f'GeckoTerminal scan error [{chain}]: {e}')

        await asyncio.sleep(60)


# =========================================================
# PUMP.FUN WEBSOCKET (Solana pre→post migration bridge)
# =========================================================
async def pumpfun_ws_loop(app: Application) -> None:
    """
    Listen to pump.fun websocket for migration events.
    When a token migrates to Raydium, fetch its full data and evaluate.
    """
    if not PUMPPORTAL_KEY:
        log.info('PumpPortal key not set — skipping pump.fun WS')
        return

    uri = f'wss://pumpportal.fun/api/data?api-key={PUMPPORTAL_KEY}'
    retries = 0
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
                retries = 0
                log.info('PumpFun WS connected')
                await ws.send(json.dumps({'method': 'subscribeNewToken'}))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        # Only care about migration events
                        if msg.get('txType') != 'create' and not msg.get('raydiumPool'):
                            continue
                        mint = msg.get('mint', '').strip()
                        if not mint or mint in alerted_set:
                            continue

                        # Token just migrated or launched — fetch full data
                        pair = await fetch_dexscreener_token(mint, 'solana')
                        if pair:
                            t = parse_dex_pair(pair, 'solana')
                            if t and t.address not in alerted_set:
                                asyncio.create_task(evaluate_and_alert(app, t))

                    except json.JSONDecodeError:
                        pass
                    except Exception as e:
                        log.debug(f'PumpFun WS event error: {e}')

        except Exception as e:
            retries += 1
            wait = min(10 * retries, 120)
            log.error(f'PumpFun WS error: {e} — retry in {wait}s')
            await asyncio.sleep(wait)


# =========================================================
# CA SCAN (manual lookup)
# =========================================================
async def scan_ca_manual(address: str) -> str:
    """Manual token lookup from Telegram — tries all chains."""
    for chain in CHAINS:
        pair = await fetch_dexscreener_token(address, chain)
        if pair:
            t = parse_dex_pair(pair, chain)
            if not t:
                continue
            await run_security_checks(t)
            if chain == 'solana':
                await enrich_from_birdeye(t)

            score, reasons = compute_score(t)
            t.score         = score
            t.score_reasons = reasons

            ok, reason  = passes_hard_filters(t)
            liq_ratio   = t.liquidity_usd / t.market_cap * 100 if t.market_cap else 0
            total_1h    = t.buys_1h + t.sells_1h
            bp_1h       = t.buys_1h / total_1h * 100 if total_1h else 0

            socials = []
            if t.twitter:       socials.append(f'[Twitter]({t.twitter})')
            if t.telegram_link: socials.append(f'[Telegram]({t.telegram_link})')
            if t.website:       socials.append(f'[Website]({t.website})')
            soc_str = ' · '.join(socials) if socials else 'None'

            sec_parts = []
            if t.has_mint_auth:   sec_parts.append('⚠️ Mint auth active')
            if t.has_freeze_auth: sec_parts.append('⚠️ Freeze auth active')
            if t.is_honeypot:     sec_parts.append('🔴 HONEYPOT')
            if t.lp_locked:       sec_parts.append(f'✅ LP locked {t.lp_lock_pct:.0f}%')
            if t.rug_score:       sec_parts.append(f'Rug score: {t.rug_score}/100')
            sec_str = ' · '.join(sec_parts) if sec_parts else 'No issues detected'

            top_pos = [r for r in reasons if r.startswith('+')][:4]
            neg     = [r for r in reasons if r.startswith('-')][:2]
            all_r   = top_pos + neg
            reasons_str = '\n'.join(f'  · {r.split(" ", 1)[1]}' for r in all_r)

            return (
                f'{chain_emoji(chain)} *{t.name}* (${t.symbol}) · {chain_name(chain)}\n'
                f'Age since migration: {age_str(t)}\n\n'
                f'💰 MC: {fmt(t.market_cap)}\n'
                f'💧 Liq: {fmt(t.liquidity_usd)} ({liq_ratio:.0f}% of MC)\n'
                f'📈 Vol: 5m {fmt(t.vol_5m)} · 1h {fmt(t.vol_1h)}\n'
                f'🔥 Buy Pressure: {bp_1h:.0f}% ({t.buys_1h}B / {t.sells_1h}S)\n'
                f'📊 Price: {fmt_pct(t.price_change_5m)} 5m · {fmt_pct(t.price_change_1h)} 1h\n'
                f'👥 Holders: {t.holder_count:,}\n\n'
                f'🔐 Security: {sec_str}\n\n'
                f'🌐 Socials: {soc_str}\n\n'
                f'🎯 Score: *{score}/100* · Filter: {"✅ PASS" if ok else f"❌ {reason}"}\n'
                f'{get_caution_label(t)} · {get_heat_label(t)}\n\n'
                f'Score breakdown:\n{reasons_str}\n\n'
                f'[DexScreener]({t.dex_url}) · `{address}`'
            )

    return (
        f'No data found for `{address}`\n\n'
        f'Token may be too new or not yet listed.\n'
        f'[Check DexScreener](https://dexscreener.com/solana/{address})'
    )


# =========================================================
# SOL PRICE
# =========================================================
SOL_PRICE = 150.0

async def update_sol_price() -> None:
    global SOL_PRICE
    url = 'https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd'
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as c:
                r = await c.get(url)
                SOL_PRICE = float(r.json()['solana']['usd'])
                log.debug(f'SOL price: ${SOL_PRICE:.2f}')
        except Exception as e:
            log.debug(f'SOL price update error: {e}')
        await asyncio.sleep(60)


# =========================================================
# CLEANUP
# =========================================================
async def cleanup_loop() -> None:
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL)
        cutoff = time.time() - 7200
        stale  = [addr for addr, t in gems.items()
                  if t.last_updated < cutoff and not t.called]
        for addr in stale:
            del gems[addr]

        # Purge alerted_set entries older than 24h
        # (We store called_at on the token)
        expired = [addr for addr in alerted_set
                   if addr in gems and (time.time() - gems[addr].called_at) > ALERT_COOLDOWN_SEC]
        for addr in expired:
            alerted_set.discard(addr)

        if stale or expired:
            log.info(f'Cleanup: removed {len(stale)} stale tokens, {len(expired)} cooldown entries')


async def log_stats() -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        alerted = sum(1 for t in gems.values() if t.called)
        log.info(
            f'STATS | tracked={len(gems)} alerted={alerted} '
            f'unique_alerted={len(alerted_set)} scans={scan_count}'
        )


# =========================================================
# TELEGRAM COMMANDS
# =========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        '*GemStalker v2* 🚀\n\n'
        'Multi-chain post-migration gem hunter.\n'
        'Chains: Solana ◎ · Base 🔵 · Ethereum ⟠\n\n'
        'Commands:\n'
        '/status  — live scanner stats\n'
        '/calls   — this month\'s alerts\n'
        '/filters — active filter settings\n'
        '/debug   — top candidates by score\n'
        '/chains  — supported chains\n\n'
        'Paste any token address to scan it manually.',
        parse_mode='Markdown',
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    alerted  = sum(1 for t in gems.values() if t.called)
    by_chain = {}
    for t in gems.values():
        by_chain[t.chain] = by_chain.get(t.chain, 0) + 1

    chain_str = '  '.join(
        f'{chain_emoji(c)} {chain_name(c)}: {n}'
        for c, n in by_chain.items()
    )
    top = sorted(
        [t for t in gems.values() if not t.called],
        key=lambda t: t.score, reverse=True
    )[:3]

    msg = (
        f'*GemStalker v2 — Status*\n\n'
        f'Tokens tracked : {len(gems)}\n'
        f'Alerts sent    : {alerted}\n'
        f'Cooldown pool  : {len(alerted_set)}\n'
        f'By chain       : {chain_str}\n\n'
    )
    if top:
        msg += '*Top candidates:*\n'
        for t in top:
            ok, r = passes_hard_filters(t)
            msg += (
                f'\n{chain_emoji(t.chain)} *{t.name}* (${t.symbol})\n'
                f'MC: {fmt(t.market_cap)} | Score: {t.score} | {t.heat_label}\n'
                f'{"✅ PASS" if ok else f"❌ {r}"}\n'
            )
    await update.message.reply_text(msg, parse_mode='Markdown')


async def cmd_debug(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    top = sorted(
        [t for t in gems.values() if not t.called],
        key=lambda t: t.score, reverse=True
    )[:5]
    if not top:
        await update.message.reply_text('No tracked tokens yet.')
        return
    lines = ['*Debug — Top 5 candidates*\n']
    for t in top:
        ok, reason = passes_hard_filters(t)
        total_1h   = t.buys_1h + t.sells_1h
        bp_1h      = t.buys_1h / total_1h * 100 if total_1h else 0
        lines.append(
            f'{chain_emoji(t.chain)} *{t.name}* | Score {t.score} | Age {age_str(t)}\n'
            f'MC={fmt(t.market_cap)} Liq={fmt(t.liquidity_usd)} BP={bp_1h:.0f}% Vol1h={fmt(t.vol_1h)}\n'
            f'{"✅ PASS" if ok else f"❌ {reason}"}\n'
            f'_{" | ".join(r.split(" ", 1)[1] for r in t.score_reasons[:3] if r.startswith("+"))}_\n'
        )
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_calls(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not recent_calls:
        await update.message.reply_text('No calls yet.')
        return
    month_start = time.mktime(time.strptime(time.strftime('%Y-%m-01'), '%Y-%m-%d'))
    month_calls = [c for c in recent_calls if c['ts'] >= month_start]
    if not month_calls:
        await update.message.reply_text('No calls this month.')
        return

    month_name = time.strftime('%B %Y')
    lines = [f'*Calls — {month_name}* ({len(month_calls)} total)\n']
    for i, c in enumerate(month_calls, 1):
        addr     = c['address']
        entry_mc = c['mc']
        mult_str = 'tracking...'
        if addr in gems:
            t = gems[addr]
            if t.market_cap > 0 and entry_mc > 0:
                mult_str = f'{t.market_cap / entry_mc:.1f}x'
        lines.append(
            f'{i}. {chain_emoji(c["chain"])} *{c["name"]}* (${c["symbol"]})\n'
            f'   Score: {c.get("score","?")} | Entry: {fmt(entry_mc)} | Now: {mult_str}'
        )
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_filters(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        '*Active Filters*\n\n'
        f'MC Range          : {fmt(MC_MIN)} – {fmt(MC_MAX)}\n'
        f'Liq/MC Ratio Min  : {LIQ_MC_RATIO_MIN:.0%}\n'
        f'Min Vol 1h        : {fmt(VOL_1H_MIN)}\n'
        f'Min Vol 5m        : {fmt(VOL_5M_MIN)}\n'
        f'Min Buy Pressure  : {BUY_PRESSURE_MIN}%\n'
        f'Min Holders       : {HOLDER_MIN}\n'
        f'Max Top Holder %  : {MAX_TOP_HOLDER_PCT}%\n'
        f'Require Socials   : {"Yes" if REQUIRE_SOCIALS else "No"}\n'
        f'Migration Only    : {"Yes" if MIGRATION_ONLY else "No"}\n'
        f'Migration Max Age : {MIGRATION_MAX_HOURS}h\n'
        f'Min Score         : {MIN_SCORE}/100\n'
        f'Alert Cooldown    : {ALERT_COOLDOWN_SEC//3600}h\n\n'
        f'Chains: {", ".join(chain_name(c) for c in CHAINS)}',
        parse_mode='Markdown',
    )


async def cmd_chains(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = ['*Supported Chains*\n']
    for c, info in CHAINS.items():
        lines.append(f'{info["emoji"]} {info["name"]}')
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text    = (update.message.text or '').strip()
    SOL_RE  = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')
    EVM_RE  = re.compile(r'\b0x[0-9a-fA-F]{40}\b')

    address = None
    m = EVM_RE.search(text)
    if m:
        address = m.group(0)
    else:
        m = SOL_RE.search(text)
        if m:
            address = m.group(0)

    if not address:
        return

    msg    = await update.message.reply_text(f'🔍 Scanning `{address[:16]}...`', parse_mode='Markdown')
    result = await scan_ca_manual(address)
    try:
        await msg.edit_text(result, parse_mode='Markdown', disable_web_page_preview=True)
    except Exception:
        await msg.edit_text(result, disable_web_page_preview=True)


# =========================================================
# HEALTH SERVER
# =========================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        alerted = sum(1 for t in gems.values() if t.called)
        self.wfile.write(
            f'GemStalker v2 OK | tracked={len(gems)} alerted={alerted}'.encode()
        )
    def log_message(self, *args):
        pass

def run_health():
    HTTPServer(('0.0.0.0', int(os.getenv('PORT', 8080))), HealthHandler).serve_forever()


# =========================================================
# STARTUP
# =========================================================
async def post_init(app: Application) -> None:
    log.info('Starting background tasks...')
    asyncio.create_task(update_sol_price())
    asyncio.create_task(scan_dexscreener(app))
    asyncio.create_task(scan_birdeye(app))
    asyncio.create_task(scan_gecko_evm(app))
    asyncio.create_task(pumpfun_ws_loop(app))
    asyncio.create_task(cleanup_loop())
    asyncio.create_task(log_stats())
    log.info(
        f'GemStalker v2 ready | '
        f'chains={list(CHAINS.keys())} | '
        f'min_score={MIN_SCORE} | '
        f'birdeye={"✓" if BIRDEYE_KEY else "✗"} | '
        f'goplus={"✓" if GOPLUS_KEY else "✗"}'
    )


def main() -> None:
    if not TG_TOKEN: raise RuntimeError('TELEGRAM_BOT_TOKEN not set')
    if not CHAT_ID:  raise RuntimeError('CHAT_ID not set')
    if not BIRDEYE_KEY:
        log.warning('BIRDEYE_API_KEY not set — Birdeye scanning disabled')
    if not GOPLUS_KEY:
        log.warning('GOPLUS_API_KEY not set — GoPlus security checks limited')
    if not PUMPPORTAL_KEY:
        log.warning('PUMPPORTAL_API_KEY not set — pump.fun WS disabled')

    threading.Thread(target=run_health, daemon=True).start()

    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler('start',   cmd_start))
    app.add_handler(CommandHandler('status',  cmd_status))
    app.add_handler(CommandHandler('calls',   cmd_calls))
    app.add_handler(CommandHandler('filters', cmd_filters))
    app.add_handler(CommandHandler('debug',   cmd_debug))
    app.add_handler(CommandHandler('chains',  cmd_chains))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.post_init = post_init

    log.info('GemStalker v2 starting...')
    app.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()
