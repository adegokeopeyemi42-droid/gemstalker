"""
GemStalker v2 — Multi-chain post-migration gem hunter
Chains: Solana, Base, Ethereum (BSC optional)
Sources: DexScreener, Birdeye, GeckoTerminal, Rugcheck/GoPlus

FIX LOG:
  [FIX-1]  Full filter failure logging with all metrics
  [FIX-2]  Relaxed filters for calibration phase
  [FIX-3]  DEBUG_MODE with full scan/score/API tracing
  [FIX-4]  Improved discovery: boosts + profiles + trending + GeckoTerminal + Raydium
  [FIX-5]  No premature hard rejection — tokens enter WATCHLIST first
  [FIX-6]  WATCHLIST pipeline (score 40-49 → watch, 50+ → alert)
  [FIX-7]  GoPlus failures → fallback to Rugcheck only, never auto-reject
  [FIX-8]  API response validation with detailed error logging
  [FIX-9]  /forcecall command bypasses all filters for live debugging
  [FIX-10] REQUIRE_SOCIALS relaxed — waived if momentum score is high
  [FIX-11] More visibility, softer thresholds, delayed re-evaluation
  [FIX-12] Solana dedicated DexScreener scanner — no longer depends on Birdeye key
            alone; added fetch_dexscreener_solana_new_pairs() + scan_solana_dex()
            so Solana tokens reach evaluate_and_alert on equal footing.
            Also fixed chainId guard in _process_pairs to log mismatches.
  [FIX-13] Fresh momentum gate (passes_momentum_check) before every alert:
            rejects late-pump tokens where 1h spike is large but 5m is cooling.
            Does NOT alter scoring or hard filters — pure timing guard.
  [FIX-14] Milestone momentum validation gate: only fires if volume is healthy
            and buy pressure >= 52%. Labels milestone 🟢 continuation or
            🟠 weak so you can act accordingly.
"""

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
from typing import Optional

import httpx
import websockets
from telegram import InlineKeyboardMarkup, InlineKeyboardButton, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# =========================================================
# CONFIG
# =========================================================
TG_TOKEN       = os.getenv('TELEGRAM_BOT_TOKEN')
CHAT_ID        = os.getenv('CHAT_ID')
BIRDEYE_KEY    = os.getenv('BIRDEYE_API_KEY', '')
GOPLUS_KEY     = os.getenv('GOPLUS_API_KEY', '')
PUMPPORTAL_KEY = os.getenv('PUMPPORTAL_API_KEY', '')

# [FIX-3] Debug mode — set True to see everything
DEBUG_MODE = True

# Polling intervals (seconds)
DEXSCREENER_POLL_INTERVAL = 30
BIRDEYE_POLL_INTERVAL     = 45
WATCHLIST_RESCAN_INTERVAL = 120   # [FIX-6] rescan watchlist every 2 min
CLEANUP_INTERVAL          = 600
STATS_INTERVAL            = 120

# Alert cooldowns
ALERT_COOLDOWN_SEC = 86400
ALERT_DELAY_SEC    = 5

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
# [FIX-2] RELAXED FILTERS — calibration phase
# =========================================================
MC_MIN              = 25_000       # was 40K
MC_MAX              = 5_000_000    # was 2M
LIQ_MC_RATIO_MIN    = 0.08         # was 0.15
VOL_1H_MIN          = 8_000        # was 20K
VOL_5M_MIN          = 1_000        # was 2K
BUY_PRESSURE_MIN    = 50           # was 55
HOLDER_MIN          = 30           # was 100
HOLDER_GROWTH_MIN   = 5
MAX_TOP_HOLDER_PCT  = 20.0
MIN_SCORE           = 50           # was 65
WATCHLIST_MIN_SCORE = 40           # [FIX-6] enter watchlist at 40+
REQUIRE_SOCIALS     = False        # [FIX-10] relaxed — see passes_hard_filters logic
SOCIALS_WAIVER_SCORE = 60          # [FIX-10] waive socials requirement if score >= this
MIGRATION_ONLY      = True
MIGRATION_MAX_HOURS = 168          # was 72 — 7 days

# [FIX-13] Momentum gate thresholds — ONLY used in passes_momentum_check()
# These do NOT affect scoring or hard filters.
MOMENTUM_LATE_PUMP_1H_THRESHOLD  = 60.0   # % — if 1h gain exceeds this...
MOMENTUM_LATE_PUMP_5M_MIN        = -2.0   # ...and 5m is below this, token is late-pump
MOMENTUM_VOL_DECAY_RATIO         = 0.15   # vol_5m / vol_1h; below this = volume drying up

# [FIX-14] Milestone momentum gate thresholds
MILESTONE_BUY_PRESSURE_MIN = 52           # % buys in last 1h to allow milestone fire
MILESTONE_VOL_DECAY_RATIO  = 0.10         # vol_5m / vol_1h minimum — avoid dead volume

# =========================================================
# LOGGING
# =========================================================
logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger(__name__)


# =========================================================
# [FIX-1] REJECTION LOGGER — always logs full metrics
# =========================================================
def log_rejection(t: 'GemToken', reason: str) -> None:
    """Log every rejection with full metrics so nothing fails silently."""
    total_1h  = t.buys_1h + t.sells_1h
    bp_1h     = t.buys_1h / total_1h * 100 if total_1h else 0
    liq_ratio = t.liquidity_usd / t.market_cap * 100 if t.market_cap else 0
    age_h     = (time.time() - t.migrated_at) / 3600

    log.info(
        f'[REJECTED] Token: {t.name} ({t.symbol}) | Chain: {t.chain} | '
        f'MC: {fmt(t.market_cap)} | Liq: {fmt(t.liquidity_usd)} ({liq_ratio:.0f}% of MC) | '
        f'Vol1h: {fmt(t.vol_1h)} | Vol5m: {fmt(t.vol_5m)} | '
        f'BuyPressure: {bp_1h:.0f}% | Holders: {t.holder_count} | '
        f'Score: {t.score} | Age: {age_h:.1f}h | '
        f'MintAuth: {t.has_mint_auth} | Honeypot: {t.is_honeypot} | '
        f'Reason: {reason}'
    )


def log_watchlist(t: 'GemToken', reason: str) -> None:
    """Log watchlist admission."""
    log.info(
        f'[WATCHLIST] Token: {t.name} ({t.symbol}) | Chain: {t.chain} | '
        f'MC: {fmt(t.market_cap)} | Score: {t.score} | Reason: {reason}'
    )


# =========================================================
# DATA MODEL
# =========================================================
@dataclass
class GemToken:
    address:          str
    chain:            str   = 'solana'
    name:             str   = 'Unknown'
    symbol:           str   = '?'
    market_cap:       float = 0.0
    liquidity_usd:    float = 0.0
    vol_5m:           float = 0.0
    vol_1h:           float = 0.0
    vol_6h:           float = 0.0
    vol_24h:          float = 0.0
    buys_5m:          int   = 0
    sells_5m:         int   = 0
    buys_1h:          int   = 0
    sells_1h:         int   = 0
    price_usd:        float = 0.0
    price_change_5m:  float = 0.0
    price_change_1h:  float = 0.0
    price_change_6h:  float = 0.0
    holder_count:     int   = 0
    holder_count_prev:int   = 0
    top_holder_pct:   float = 0.0
    has_mint_auth:    bool  = False
    has_freeze_auth:  bool  = False
    is_honeypot:      bool  = False
    lp_locked:        bool  = False
    lp_lock_pct:      float = 0.0
    rug_score:        int   = 0
    twitter:          str   = ''
    telegram_link:    str   = ''
    website:          str   = ''
    dex_url:          str   = ''
    pair_address:     str   = ''
    dex_id:           str   = ''
    created_at:       float = field(default_factory=time.time)
    migrated_at:      float = field(default_factory=time.time)
    last_updated:     float = field(default_factory=time.time)
    last_scanned:     float = 0.0
    score:            int   = 0
    score_reasons:    list  = field(default_factory=list)
    caution_label:    str   = ''
    heat_label:       str   = ''
    called:           bool  = False
    called_at:        float = 0.0
    entry_mc:         float = 0.0
    peak_mult:        float = 1.0
    next_milestone:   int   = 2
    # [FIX-6] watchlist state
    in_watchlist:     bool  = False
    watchlist_added:  float = 0.0
    watchlist_rescans: int  = 0
    # security check tracking [FIX-7]
    security_checked: bool  = False
    goplus_failed:    bool  = False
    rugcheck_failed:  bool  = False
    vol_history:      deque = field(default_factory=lambda: deque(maxlen=10))
    holder_history:   deque = field(default_factory=lambda: deque(maxlen=10))

MILESTONES = [2, 3, 5, 10, 25, 50, 100]

# Global state
gems:         dict  = {}
watchlist:    dict  = {}           # [FIX-6] address -> GemToken
alerted_set:  set   = set()
recent_calls: deque = deque(maxlen=500)
scan_count:   int   = 0


# =========================================================
# SCORING ENGINE  ← UNCHANGED
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
    elif t.vol_1h >= 8_000:
        score += 4;  reasons.append(f'+4 vol_1h=${t.vol_1h/1000:.0f}K')

    # 5m spike bonus
    if t.vol_5m >= 10_000:
        score += 10; reasons.append(f'+10 vol_5m spike=${t.vol_5m/1000:.1f}K')
    elif t.vol_5m >= 5_000:
        score += 5;  reasons.append(f'+5 vol_5m=${t.vol_5m/1000:.1f}K')
    elif t.vol_5m >= 1_000:
        score += 2;  reasons.append(f'+2 vol_5m=${t.vol_5m/1000:.1f}K')

    # 2. Liquidity quality (+20 max)
    liq_ratio = t.liquidity_usd / t.market_cap if t.market_cap > 0 else 0
    if liq_ratio >= 0.40:
        score += 20; reasons.append(f'+20 liq_ratio={liq_ratio:.0%}')
    elif liq_ratio >= 0.25:
        score += 14; reasons.append(f'+14 liq_ratio={liq_ratio:.0%}')
    elif liq_ratio >= 0.15:
        score += 8;  reasons.append(f'+8 liq_ratio={liq_ratio:.0%}')
    elif liq_ratio >= 0.08:
        score += 4;  reasons.append(f'+4 liq_ratio={liq_ratio:.0%}')

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
    elif avg_bp >= 50:
        score += 2;  reasons.append(f'+2 buy_pressure={avg_bp:.0f}%')

    # 4. Holder growth (+15 max)
    holder_growth = t.holder_count - t.holder_count_prev
    if t.holder_count >= 2000:
        score += 10; reasons.append(f'+10 holders={t.holder_count}')
    elif t.holder_count >= 500:
        score += 6;  reasons.append(f'+6 holders={t.holder_count}')
    elif t.holder_count >= 100:
        score += 3;  reasons.append(f'+3 holders={t.holder_count}')
    elif t.holder_count >= 30:
        score += 1;  reasons.append(f'+1 holders={t.holder_count}')
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
    if t.vol_1h > 500_000 and t.holder_count < 50:
        score -= 25; reasons.append('-25 suspected_wash_trading')
    if avg_bp < 40:
        score -= 15; reasons.append(f'-15 heavy_sell_pressure={avg_bp:.0f}%')

    return max(score, 0), reasons


def passes_hard_filters(t: GemToken) -> tuple[bool, str]:
    """[FIX-1] Returns (pass, reason) — caller must log rejections."""
    if t.market_cap < MC_MIN:
        return False, f'MC too low ({fmt(t.market_cap)} < {fmt(MC_MIN)})'
    if t.market_cap > MC_MAX:
        return False, f'MC too high ({fmt(t.market_cap)} > {fmt(MC_MAX)})'
    if t.liquidity_usd <= 0:
        return False, 'No liquidity data'
    liq_ratio = t.liquidity_usd / t.market_cap
    if liq_ratio < LIQ_MC_RATIO_MIN:
        return False, f'Liq/MC ratio too low ({liq_ratio:.1%} < {LIQ_MC_RATIO_MIN:.0%})'
    if t.vol_1h < VOL_1H_MIN:
        return False, f'Vol 1h too low ({fmt(t.vol_1h)} < {fmt(VOL_1H_MIN)})'
    if t.vol_5m < VOL_5M_MIN:
        return False, f'Vol 5m too low ({fmt(t.vol_5m)} < {fmt(VOL_5M_MIN)})'
    total_1h = t.buys_1h + t.sells_1h
    bp_1h = t.buys_1h / total_1h * 100 if total_1h else 0
    if bp_1h < BUY_PRESSURE_MIN:
        return False, f'Buy pressure too low ({bp_1h:.0f}% < {BUY_PRESSURE_MIN}%)'
    if t.holder_count < HOLDER_MIN:
        return False, f'Holders too low ({t.holder_count} < {HOLDER_MIN})'
    # [FIX-10] Socials: waive if score is high enough
    has_socials = any([t.twitter, t.telegram_link, t.website])
    if not has_socials and t.score < SOCIALS_WAIVER_SCORE:
        return False, f'No socials and score {t.score} < {SOCIALS_WAIVER_SCORE} (waiver threshold)'
    if t.is_honeypot:
        return False, 'Honeypot detected'
    if t.has_mint_auth:
        return False, 'Mint authority active'
    age_hours = (time.time() - t.migrated_at) / 3600
    if MIGRATION_ONLY and age_hours > MIGRATION_MAX_HOURS:
        return False, f'Too old ({age_hours:.0f}h > {MIGRATION_MAX_HOURS}h since migration)'
    return True, 'OK'


# =========================================================
# [FIX-13] MOMENTUM GATE — timing check only, not a hard filter
# Called once just before send_alert. Does NOT affect scoring.
# =========================================================
def passes_momentum_check(t: GemToken) -> tuple[bool, str]:
    """
    Detects late-pump / post-ATH conditions and blocks the alert.
    Returns (True, 'OK') to proceed, or (False, reason) to suppress.

    Conditions that indicate a late call:
      A) Large 1h spike + cooling 5m price  → exhaustion
      B) Volume drying up fast relative to 1h volume → distribution phase
    """
    # Guard: if we have no 5m data at all, don't block — let it through
    if t.vol_5m == 0 and t.price_change_5m == 0:
        return True, 'OK (no 5m data, skipping momentum gate)'

    # Condition A: strong 1h move but 5m is already negative — late pump
    if (t.price_change_1h > MOMENTUM_LATE_PUMP_1H_THRESHOLD and
            t.price_change_5m < MOMENTUM_LATE_PUMP_5M_MIN):
        reason = (
            f'Late-pump suppressed: 1h=+{t.price_change_1h:.1f}% but '
            f'5m={t.price_change_5m:.1f}% (cooling after spike)'
        )
        log.info(f'[MOMENTUM-GATE] {t.name} ({t.symbol}) | {reason}')
        return False, reason

    # Condition B: vol_5m is tiny fraction of vol_1h — volume exhaustion
    if t.vol_1h > 0:
        vol_ratio = t.vol_5m / t.vol_1h
        if vol_ratio < MOMENTUM_VOL_DECAY_RATIO and t.price_change_1h > 30:
            reason = (
                f'Volume exhaustion suppressed: vol_5m/vol_1h={vol_ratio:.2f} '
                f'(< {MOMENTUM_VOL_DECAY_RATIO}) with 1h=+{t.price_change_1h:.1f}%'
            )
            log.info(f'[MOMENTUM-GATE] {t.name} ({t.symbol}) | {reason}')
            return False, reason

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
# [FIX-8] API RESPONSE VALIDATION
# =========================================================
def validate_api_response(source: str, url: str, response, expected_type=dict) -> Optional[dict]:
    """Validate and log issues with API responses."""
    if response is None:
        log.warning(f'[API-FAIL] {source} | URL: {url} | Empty response (None)')
        return None
    if hasattr(response, 'status_code'):
        if response.status_code == 429:
            log.warning(f'[API-FAIL] {source} | URL: {url} | Rate limited (429)')
            return None
        if response.status_code >= 400:
            log.warning(f'[API-FAIL] {source} | URL: {url} | HTTP {response.status_code}')
            return None
    try:
        data = response.json() if hasattr(response, 'json') else response
        if not isinstance(data, expected_type) and expected_type is dict:
            if isinstance(data, list) and expected_type is list:
                return data
            log.warning(f'[API-FAIL] {source} | URL: {url} | Unexpected type: {type(data).__name__}')
            return None
        return data
    except Exception as e:
        log.warning(f'[API-FAIL] {source} | URL: {url} | JSON parse error: {e}')
        return None


# =========================================================
# ALERT BUILDER  ← UNCHANGED
# =========================================================
def build_alert(t: GemToken, is_watchlist_promo: bool = False) -> str:
    total_1h  = t.buys_1h + t.sells_1h
    bp_1h     = t.buys_1h / total_1h * 100 if total_1h else 0
    liq_ratio = t.liquidity_usd / t.market_cap * 100 if t.market_cap else 0
    holder_delta = t.holder_count - t.holder_count_prev

    socials = []
    if t.twitter:       socials.append(f'[Twitter/X]({t.twitter})')
    if t.telegram_link: socials.append(f'[Telegram]({t.telegram_link})')
    if t.website:       socials.append(f'[Website]({t.website})')
    soc_str = ' · '.join(socials) if socials else 'None'

    top_reasons = [r for r in t.score_reasons if r.startswith('+')][:5]
    reasons_str = '\n'.join(f'  · {r.split(" ", 1)[1]}' for r in top_reasons)

    sec_flags = []
    if t.lp_locked:           sec_flags.append(f'LP Locked {t.lp_lock_pct:.0f}%')
    if not t.has_mint_auth:   sec_flags.append('No Mint Auth ✓')
    if not t.has_freeze_auth: sec_flags.append('No Freeze Auth ✓')
    if t.rug_score > 0:       sec_flags.append(f'Rug Score: {t.rug_score}/100')
    if t.goplus_failed:       sec_flags.append('⚠️ GoPlus unavailable')
    sec_str = ' · '.join(sec_flags) if sec_flags else 'Not verified'

    holder_str = f'{t.holder_count:,}'
    if holder_delta > 0:
        holder_str += f' (+{holder_delta} recent)'

    header = '🚨 *MIGRATION GEM DETECTED* 🚨'
    if is_watchlist_promo:
        header = '📈 *WATCHLIST BREAKOUT* 🚨'

    lines = [
        header,
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
    url = f'https://api.dexscreener.com/latest/dex/tokens/{address}'
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(url)
            data = validate_api_response('DexScreener/token', url, r)
            if not data:
                return None
            pairs = [p for p in (data.get('pairs') or [])
                     if p.get('chainId') == chain]
            if not pairs:
                if DEBUG_MODE:
                    log.debug(f'[API] DexScreener: no pairs for {address} on {chain}')
                return None
            return max(pairs, key=lambda p: float((p.get('liquidity') or {}).get('usd', 0)))
    except httpx.TimeoutException:
        log.warning(f'[API-FAIL] DexScreener/token | URL: {url} | Timeout')
        return None
    except Exception as e:
        log.debug(f'[API-FAIL] DexScreener/token | {address}: {e}')
        return None


async def fetch_dexscreener_trending(chain: str) -> list[dict]:
    url = 'https://api.dexscreener.com/latest/dex/search'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url, params={'q': chain})
            data = validate_api_response('DexScreener/trending', url, r)
            if not data:
                return []
            pairs = [p for p in (data.get('pairs') or []) if p.get('chainId') == chain]
            if DEBUG_MODE:
                log.debug(f'[API] DexScreener trending [{chain}]: {len(pairs)} pairs')
            return pairs
    except Exception as e:
        log.warning(f'[API-FAIL] DexScreener/trending [{chain}]: {e}')
        return []


async def fetch_dexscreener_new_pairs(chain: str) -> list[dict]:
    """[FIX-4] Boosted/new token listings."""
    url = 'https://api.dexscreener.com/token-boosts/latest/v1'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url)
            data = r.json()
            if isinstance(data, list):
                result = [b for b in data if b.get('chainId') == chain]
                if DEBUG_MODE:
                    log.debug(f'[API] DexScreener boosts [{chain}]: {len(result)} entries')
                return result
            return []
    except Exception as e:
        log.warning(f'[API-FAIL] DexScreener/boosts: {e}')
        return []


async def fetch_dexscreener_token_profiles(chain: str) -> list[dict]:
    """[FIX-4] Latest token profiles — additional discovery source."""
    url = 'https://api.dexscreener.com/token-profiles/latest/v1'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url)
            data = r.json()
            if isinstance(data, list):
                result = [p for p in data if p.get('chainId') == chain]
                if DEBUG_MODE:
                    log.debug(f'[API] DexScreener profiles [{chain}]: {len(result)} entries')
                return result
            return []
    except Exception as e:
        log.warning(f'[API-FAIL] DexScreener/profiles: {e}')
        return []


# =========================================================
# [FIX-12] SOLANA-SPECIFIC DEXSCREENER SCANNER
# Fetches the /latest/dex/pairs/solana endpoint directly so
# Solana tokens are discovered even when Birdeye key is absent.
# This is additive — it does not replace any existing source.
# =========================================================
async def fetch_dexscreener_solana_new_pairs() -> list[dict]:
    """
    [FIX-12] Pull fresh Solana pairs directly from DexScreener's
    chain-specific pairs endpoint. Returns raw pair dicts that can
    go straight into parse_dex_pair().
    """
    url = 'https://api.dexscreener.com/latest/dex/pairs/solana'
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.get(url)
            data = validate_api_response('DexScreener/solana_pairs', url, r)
            if not data:
                return []
            pairs = data.get('pairs') or []
            # Defensive: ensure every pair is tagged solana (they should be)
            pairs = [p for p in pairs if p.get('chainId', 'solana') == 'solana']
            if DEBUG_MODE:
                log.debug(f'[API] DexScreener solana_pairs: {len(pairs)} pairs')
            return pairs
    except Exception as e:
        log.warning(f'[API-FAIL] DexScreener/solana_pairs: {e}')
        return []


async def fetch_raydium_new_pools() -> list[str]:
    """[FIX-4] Fetch newly created Raydium pools (Solana)."""
    url = 'https://api-v3.raydium.io/pools/info/list'
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(url, params={
                'poolType': 'standard',
                'poolSortField': 'default',
                'sortType': 'desc',
                'pageSize': 50,
                'page': 1,
            })
            data = r.json()
            pools = data.get('data', {}).get('data', [])
            # Extract base token mint addresses
            mints = []
            for p in pools:
                mint = p.get('mintA', {}).get('address') or p.get('baseMint')
                if mint:
                    mints.append(mint)
            if DEBUG_MODE:
                log.debug(f'[API] Raydium new pools: {len(mints)} mints')
            return mints
    except Exception as e:
        log.warning(f'[API-FAIL] Raydium/new_pools: {e}')
        return []


def parse_dex_pair(pair: dict, chain: str) -> Optional['GemToken']:
    try:
        base     = pair.get('baseToken', {})
        address  = base.get('address', '')
        if not address:
            return None

        liq = float((pair.get('liquidity') or {}).get('usd', 0) or 0)
        mc  = float(pair.get('fdv') or pair.get('marketCap') or 0)
        if mc == 0 and liq > 0:
            mc = liq * 3

        txns = pair.get('txns') or {}
        vol  = pair.get('volume') or {}
        pc   = pair.get('priceChange') or {}
        info = pair.get('info') or {}
        socials_raw  = info.get('socials') or []
        websites_raw = info.get('websites') or []

        buys_5m  = int((txns.get('m5') or {}).get('buys', 0))
        sells_5m = int((txns.get('m5') or {}).get('sells', 0))
        buys_1h  = int((txns.get('h1') or {}).get('buys', 0))
        sells_1h = int((txns.get('h1') or {}).get('sells', 0))

        twitter = next((s.get('url','') for s in socials_raw if s.get('type','').lower() in ('twitter','x')), '')
        tg      = next((s.get('url','') for s in socials_raw if s.get('type','').lower() == 'telegram'), '')
        website = next((w.get('url','') for w in websites_raw), '')

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

        if DEBUG_MODE:
            liq_ratio = liq / mc * 100 if mc else 0
            log.debug(
                f'[PARSED] {t.name} ({t.symbol}) | {chain} | '
                f'MC={fmt(mc)} Liq={fmt(liq)} ({liq_ratio:.0f}%) '
                f'Vol1h={fmt(t.vol_1h)} Vol5m={fmt(t.vol_5m)} '
                f'Buys1h={buys_1h} Sells1h={sells_1h}'
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
    url = f'https://public-api.birdeye.so/defi/token_overview'
    try:
        headers = {'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana'}
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(url, headers=headers, params={'address': address})
            data = validate_api_response('Birdeye/token', url, r)
            return data.get('data') if data else None
    except Exception as e:
        log.debug(f'[API-FAIL] Birdeye/token | {address}: {e}')
        return None


async def fetch_birdeye_trending() -> list[dict]:
    if not BIRDEYE_KEY:
        return []
    url = 'https://public-api.birdeye.so/defi/trending_tokens'
    try:
        headers = {'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana'}
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(url, headers=headers,
                           params={'sort_by': 'volume1hUSD', 'sort_type': 'desc', 'limit': 50})
            data = validate_api_response('Birdeye/trending', url, r)
            result = data.get('data', {}).get('tokens', []) if data else []
            if DEBUG_MODE:
                log.debug(f'[API] Birdeye trending: {len(result)} tokens')
            return result
    except Exception as e:
        log.warning(f'[API-FAIL] Birdeye/trending: {e}')
        return []


async def fetch_birdeye_new_listings() -> list[dict]:
    if not BIRDEYE_KEY:
        return []
    url = 'https://public-api.birdeye.so/defi/new_listing'
    try:
        headers = {'X-API-KEY': BIRDEYE_KEY, 'x-chain': 'solana'}
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(url, headers=headers,
                           params={'limit': 50, 'time_to': int(time.time()),
                                   'time_from': int(time.time()) - 3600})
            data = validate_api_response('Birdeye/new_listing', url, r)
            result = data.get('data', {}).get('items', []) if data else []
            if DEBUG_MODE:
                log.debug(f'[API] Birdeye new listings: {len(result)} tokens')
            return result
    except Exception as e:
        log.warning(f'[API-FAIL] Birdeye/new_listing: {e}')
        return []


async def enrich_from_birdeye(t: GemToken) -> None:
    if t.chain != 'solana' or not BIRDEYE_KEY:
        return
    data = await fetch_birdeye_token(t.address)
    if not data:
        if DEBUG_MODE:
            log.debug(f'[API] Birdeye enrich skipped for {t.address} (no data)')
        return
    t.holder_count = int(data.get('holder', t.holder_count) or t.holder_count)


# =========================================================
# [FIX-7] RUGCHECK / GOPLUS SECURITY — with proper fallbacks
# =========================================================
async def fetch_rugcheck(address: str) -> dict:
    url = f'https://api.rugcheck.xyz/v1/tokens/{address}/report/summary'
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(url)
            data = validate_api_response('Rugcheck', url, r)
            if not data:
                return {'_failed': True}
            score = data.get('score', 0)
            risks = data.get('risks', [])
            has_mint   = any('mint' in str(r).lower() for r in risks)
            has_freeze = any('freeze' in str(r).lower() for r in risks)
            if DEBUG_MODE:
                log.debug(f'[SECURITY] Rugcheck {address}: score={score} mint={has_mint} freeze={has_freeze}')
            return {'rug_score': score, 'has_mint_auth': has_mint, 'has_freeze_auth': has_freeze}
    except httpx.TimeoutException:
        log.warning(f'[API-FAIL] Rugcheck | {address} | Timeout')
        return {'_failed': True}
    except Exception as e:
        log.debug(f'[API-FAIL] Rugcheck | {address}: {e}')
        return {'_failed': True}


async def fetch_goplus(address: str, chain: str) -> dict:
    chain_map = {'ethereum': '1', 'bsc': '56', 'base': '8453', 'solana': 'solana'}
    chain_id  = chain_map.get(chain, '1')
    base_url  = ('https://api.gopluslabs.io/api/v1/token_security/'
                 if chain != 'solana' else
                 'https://api.gopluslabs.io/api/v1/solana/token_security/')
    url = f'{base_url}{chain_id}' if chain != 'solana' else base_url
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            params = {'contract_addresses': address}
            if GOPLUS_KEY:
                params['access_token'] = GOPLUS_KEY
            r    = await c.get(url, params=params)
            data = validate_api_response('GoPlus', url, r)
            if not data:
                return {'_failed': True}
            result = data.get('result', {}).get(address.lower(), {})
            if not result:
                if DEBUG_MODE:
                    log.debug(f'[API] GoPlus: empty result for {address}')
                return {'_failed': True}
            output = {
                'is_honeypot':    result.get('is_honeypot') == '1',
                'has_mint_auth':  result.get('can_take_back_ownership') == '1' or result.get('mintable') == '1',
                'has_freeze_auth':result.get('transfer_pausable') == '1',
                'lp_locked':      float(result.get('lp_locked_percent', 0) or 0) > 0,
                'lp_lock_pct':    float(result.get('lp_locked_percent', 0) or 0),
                'top_holder_pct': float(result.get('top10_holder_rate', 0) or 0) * 100,
                'holder_count':   int(result.get('holder_count', 0) or 0),
            }
            if DEBUG_MODE:
                log.debug(f'[SECURITY] GoPlus {address}: honeypot={output["is_honeypot"]} mint={output["has_mint_auth"]}')
            return output
    except httpx.TimeoutException:
        log.warning(f'[API-FAIL] GoPlus | {address} | Timeout')
        return {'_failed': True}
    except Exception as e:
        log.debug(f'[API-FAIL] GoPlus | {address}: {e}')
        return {'_failed': True}


async def run_security_checks(t: GemToken) -> None:
    """
    [FIX-7] Run security checks with proper fallbacks.
    GoPlus failure does NOT auto-reject. Falls back to Rugcheck only.
    """
    tasks = []
    if t.chain == 'solana':
        tasks.append(fetch_rugcheck(t.address))
    tasks.append(fetch_goplus(t.address, t.chain))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    any_success = False
    for result in results:
        if isinstance(result, Exception):
            log.warning(f'[SECURITY] Exception during security check for {t.address}: {result}')
            continue
        if isinstance(result, dict):
            if result.get('_failed'):
                # Track which checks failed [FIX-7]
                if 'rug_score' in result or 'has_mint_auth' in result:
                    t.rugcheck_failed = True
                else:
                    t.goplus_failed = True
                log.debug(f'[SECURITY] Check failed for {t.address} — not rejecting token')
                continue
            any_success = True
            for key, val in result.items():
                if key.startswith('_'):
                    continue
                if hasattr(t, key) and val is not None:
                    if key == 'lp_locked' and t.lp_locked:
                        continue
                    setattr(t, key, val)

    t.security_checked = True
    if not any_success:
        log.info(f'[SECURITY] All checks failed for {t.address} — proceeding with no security data')


# =========================================================
# GECKOTERM INAL (EVM chains)
# =========================================================
async def fetch_gecko_new_pools(chain: str) -> list[dict]:
    gecko_chains = {'ethereum': 'eth', 'base': 'base', 'solana': 'solana', 'bsc': 'bsc'}
    g_chain = gecko_chains.get(chain, chain)
    url = f'https://api.geckoterminal.com/api/v2/networks/{g_chain}/new_pools'
    try:
        async with httpx.AsyncClient(timeout=12) as c:
            r = await c.get(url, params={'page': 1},
                           headers={'Accept': 'application/json;version=20230302'})
            data = validate_api_response('GeckoTerminal', url, r)
            if not data:
                return []
            pools = data.get('data', [])
            if DEBUG_MODE:
                log.debug(f'[API] GeckoTerminal new pools [{chain}]: {len(pools)} pools')
            return pools
    except Exception as e:
        log.warning(f'[API-FAIL] GeckoTerminal [{chain}]: {e}')
        return []


async def gecko_pool_to_token(pool: dict, chain: str) -> Optional[GemToken]:
    try:
        attrs    = pool.get('attributes', {})
        rels     = pool.get('relationships', {})
        base_tok = rels.get('base_token', {}).get('data', {})
        token_id = base_tok.get('id', '')
        address  = token_id.split('_', 1)[-1] if '_' in token_id else token_id
        if not address:
            return None

        mc      = float(attrs.get('fully_diluted_valuation') or 0)
        liq     = float(attrs.get('reserve_in_usd') or 0)
        vol_24h = float(attrs.get('volume_usd', {}).get('h24', 0) or 0)
        vol_1h  = float(attrs.get('volume_usd', {}).get('h1', 0) or 0)
        created = attrs.get('pool_created_at')
        created_ts = time.time()
        if created:
            from datetime import datetime, timezone
            try:
                created_ts = datetime.fromisoformat(created.replace('Z', '+00:00')).timestamp()
            except Exception:
                pass

        name   = attrs.get('name', 'Unknown').split('/')[0].strip()
        symbol = name[:10]

        return GemToken(
            address=address, chain=chain, name=name, symbol=symbol,
            market_cap=mc, liquidity_usd=liq, vol_1h=vol_1h, vol_24h=vol_24h,
            migrated_at=created_ts, created_at=created_ts,
            dex_url=f'https://dexscreener.com/{chain}/{address}',
        )
    except Exception as e:
        log.debug(f'gecko_pool_to_token error: {e}')
        return None


# =========================================================
# [FIX-6] WATCHLIST PIPELINE
# =========================================================
def add_to_watchlist(t: GemToken) -> None:
    """Add token to watchlist for delayed re-evaluation."""
    if t.address in watchlist or t.address in alerted_set:
        return
    t.in_watchlist    = True
    t.watchlist_added = time.time()
    watchlist[t.address] = t
    log_watchlist(t, f'score={t.score} (below alert threshold {MIN_SCORE})')


async def rescan_watchlist(app: Application) -> None:
    """[FIX-6] Periodically re-evaluate watchlist tokens."""
    while True:
        await asyncio.sleep(WATCHLIST_RESCAN_INTERVAL)
        if not watchlist:
            continue

        log.info(f'[WATCHLIST] Rescanning {len(watchlist)} tokens...')
        expired = []

        for address, t in list(watchlist.items()):
            if address in alerted_set:
                expired.append(address)
                continue
            # Expire after 30 mins in watchlist
            if time.time() - t.watchlist_added > 1800:
                log.debug(f'[WATCHLIST] Expired: {t.name} ({t.symbol}) after 30m')
                expired.append(address)
                continue

            t.watchlist_rescans += 1
            # Refresh data from DexScreener
            pair = await fetch_dexscreener_token(address, t.chain)
            if not pair:
                continue

            fresh = parse_dex_pair(pair, t.chain)
            if not fresh:
                continue

            # Preserve history
            fresh.holder_count_prev = t.holder_count
            fresh.watchlist_added   = t.watchlist_added
            fresh.watchlist_rescans = t.watchlist_rescans
            fresh.in_watchlist      = True
            fresh.goplus_failed     = t.goplus_failed
            fresh.rugcheck_failed   = t.rugcheck_failed
            fresh.security_checked  = t.security_checked

            # Re-score
            score, reasons = compute_score(fresh)
            fresh.score         = score
            fresh.score_reasons = reasons
            fresh.caution_label = get_caution_label(fresh)
            fresh.heat_label    = get_heat_label(fresh)
            watchlist[address]  = fresh

            log.debug(
                f'[WATCHLIST] Rescan #{fresh.watchlist_rescans}: {fresh.name} ({fresh.symbol}) '
                f'score={score} vol1h={fmt(fresh.vol_1h)} bp={fresh.buys_1h}/{fresh.buys_1h+fresh.sells_1h}'
            )

            if score >= MIN_SCORE:
                ok, reason = passes_hard_filters(fresh)
                if ok:
                    log.info(f'[WATCHLIST→ALERT] {fresh.name} promoted! score={score}')
                    expired.append(address)
                    asyncio.create_task(evaluate_and_alert(app, fresh, is_watchlist_promo=True))
                else:
                    log_rejection(fresh, f'WATCHLIST promotion blocked: {reason}')

        for addr in expired:
            watchlist.pop(addr, None)


# =========================================================
# CORE EVALUATION PIPELINE
# =========================================================
async def evaluate_and_alert(
    app: Application,
    t: GemToken,
    is_watchlist_promo: bool = False,
) -> None:
    """[FIX-5] Full pipeline: security → score → watchlist/alert (no premature rejection)."""
    if t.address in alerted_set:
        return

    # Security checks
    if not t.security_checked:
        await run_security_checks(t)

    # Birdeye enrich (Solana) — [FIX-12] this is supplemental only;
    # Solana tokens continue even when Birdeye is unavailable.
    if t.chain == 'solana':
        await enrich_from_birdeye(t)

    # Score
    score, reasons = compute_score(t)
    t.score         = score
    t.score_reasons = reasons
    t.caution_label = get_caution_label(t)
    t.heat_label    = get_heat_label(t)
    t.last_updated  = time.time()

    if DEBUG_MODE:
        log.debug(
            f'[SCORE] {t.name} ({t.symbol}) | {t.chain} | score={score} | '
            f'reasons: {" | ".join(t.score_reasons[:5])}'
        )

    # Hard filters
    ok, reason = passes_hard_filters(t)
    if not ok:
        log_rejection(t, reason)
        # [FIX-5] If score is promising, put on watchlist instead of discarding
        if score >= WATCHLIST_MIN_SCORE and not is_watchlist_promo:
            add_to_watchlist(t)
        return

    # Score threshold
    if score < MIN_SCORE:
        log.debug(f'[SCORE-FAIL] {t.name}: score {score} < {MIN_SCORE}')
        # [FIX-6] Watchlist if score is borderline
        if score >= WATCHLIST_MIN_SCORE and not is_watchlist_promo:
            add_to_watchlist(t)
        return

    # Duplicate check
    if t.address in alerted_set:
        return

    # Store in gems dict
    gems[t.address] = t

    # Delay + re-validate
    await asyncio.sleep(ALERT_DELAY_SEC)
    ok, reason = passes_hard_filters(t)
    if not ok:
        log.info(f'[ALERT BLOCKED post-delay] {t.name} | {reason}')
        return
    score, reasons = compute_score(t)
    if score < MIN_SCORE:
        log.info(f'[ALERT BLOCKED post-delay score] {t.name} | {score}')
        return

    # [FIX-13] Momentum gate — timing check, does not alter score or hard filters
    mom_ok, mom_reason = passes_momentum_check(t)
    if not mom_ok:
        log.info(f'[MOMENTUM-GATE BLOCKED] {t.name} ({t.symbol}) | {mom_reason}')
        # Still watchlist it — conditions may improve on next rescan
        if not is_watchlist_promo:
            add_to_watchlist(t)
        return

    await send_alert(app, t, is_watchlist_promo=is_watchlist_promo)


async def send_alert(
    app: Application,
    t: GemToken,
    is_watchlist_promo: bool = False,
) -> None:
    dex_url = t.dex_url or f'https://dexscreener.com/{t.chain}/{t.address}'
    buttons = [[InlineKeyboardButton('📊 DexScreener', url=dex_url)]]
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
            text       = build_alert(t, is_watchlist_promo=is_watchlist_promo),
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
            + (' [WATCHLIST PROMO]' if is_watchlist_promo else '')
        )
    except Exception as e:
        log.error(f'Alert send error: {e}')


# =========================================================
# [FIX-14] MILESTONE TRACKER with momentum validation gate
# =========================================================
def _get_milestone_label(t: GemToken) -> str:
    """
    [FIX-14] Returns 🟢 or 🟠 label based on current momentum health.
    Does NOT block the milestone — just informs you of quality.
    """
    total_1h = t.buys_1h + t.sells_1h
    bp_1h    = t.buys_1h / total_1h * 100 if total_1h else 0
    vol_ratio = t.vol_5m / t.vol_1h if t.vol_1h > 0 else 0

    healthy_bp  = bp_1h >= MILESTONE_BUY_PRESSURE_MIN
    healthy_vol = vol_ratio >= MILESTONE_VOL_DECAY_RATIO

    if healthy_bp and healthy_vol:
        return '🟢 CONTINUATION — momentum healthy'
    return '🟠 WEAK MILESTONE — possible reversal risk'


async def check_milestones(app: Application, t: GemToken) -> None:
    if not t.called or not t.entry_mc or not t.next_milestone:
        return
    if t.market_cap <= 0:
        return
    mult     = t.market_cap / t.entry_mc
    t.peak_mult = max(t.peak_mult, mult)
    if mult >= t.next_milestone:
        m   = t.next_milestone
        nxt = next((x for x in MILESTONES if x > m), None)
        t.next_milestone = nxt

        # [FIX-14] Momentum validation gate
        total_1h  = t.buys_1h + t.sells_1h
        bp_1h     = t.buys_1h / total_1h * 100 if total_1h else 0
        vol_ratio = t.vol_5m / t.vol_1h if t.vol_1h > 0 else 1.0  # default pass if no data

        if bp_1h > 0 and bp_1h < MILESTONE_BUY_PRESSURE_MIN and vol_ratio < MILESTONE_VOL_DECAY_RATIO:
            # Both indicators weak: suppress milestone spam
            log.info(
                f'[MILESTONE SUPPRESSED] {t.name} {m}x | '
                f'bp={bp_1h:.0f}% vol_ratio={vol_ratio:.2f} — looks like dead-cat / sideways'
            )
            return

        milestone_label = _get_milestone_label(t)

        try:
            await app.bot.send_message(
                chat_id=CHAT_ID, parse_mode='Markdown',
                text=(
                    f'🏆 *{m}x MILESTONE HIT*\n\n'
                    f'{chain_emoji(t.chain)} *{t.name}* (${t.symbol})\n'
                    f'Entry : {fmt(t.entry_mc)}\n'
                    f'Now   : {fmt(t.market_cap)}\n'
                    f'*{mult:.1f}x* from call  ·  Peak: {t.peak_mult:.1f}x\n\n'
                    f'{milestone_label}\n\n'
                    f'`{t.address}`'
                ),
            )
        except Exception as e:
            log.error(f'Milestone error: {e}')


# =========================================================
# [FIX-4] + [FIX-12] IMPROVED SCAN LOOPS
# =========================================================
async def scan_dexscreener(app: Application) -> None:
    while True:
        global scan_count
        scan_count += 1
        await asyncio.gather(
            *[_scan_dex_chain(app, chain) for chain in CHAINS],
            return_exceptions=True,
        )
        await asyncio.sleep(DEXSCREENER_POLL_INTERVAL)


async def _scan_dex_chain(app: Application, chain: str) -> None:
    try:
        # Source 1: Boosted tokens
        boosts = await fetch_dexscreener_new_pairs(chain)
        await _process_address_list(app, [b.get('tokenAddress') for b in boosts[:30]], chain)

        # Source 2: Token profiles [FIX-4]
        profiles = await fetch_dexscreener_token_profiles(chain)
        await _process_address_list(app, [p.get('tokenAddress') for p in profiles[:30]], chain)

        # Source 3: Trending search pairs
        pairs = await fetch_dexscreener_trending(chain)
        _process_pairs(app, pairs, chain)

    except Exception as e:
        log.error(f'[SCAN] DexScreener error [{chain}]: {e}')


async def _process_address_list(app: Application, addresses: list, chain: str) -> None:
    for address in addresses:
        if not address or address in alerted_set:
            continue
        pair = await fetch_dexscreener_token(address, chain)
        if not pair:
            continue
        t = parse_dex_pair(pair, chain)
        if not t:
            continue
        existing = gems.get(t.address)
        if existing and existing.called:
            existing.market_cap = t.market_cap
            await check_milestones(app, existing)
        elif t.address not in alerted_set:
            asyncio.create_task(evaluate_and_alert(app, t))


def _process_pairs(app: Application, pairs: list, chain: str) -> None:
    for pair in pairs[:50]:
        # [FIX-12] Log chainId mismatches instead of silently dropping
        pair_chain = pair.get('chainId', '')
        if pair_chain != chain:
            if DEBUG_MODE:
                log.debug(
                    f'[SCAN] chainId mismatch: expected={chain} got={pair_chain} '
                    f'pair={pair.get("pairAddress","?")} — skipping'
                )
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


# =========================================================
# [FIX-12] DEDICATED SOLANA SCANNER
# Runs independently — does not depend on BIRDEYE_KEY.
# Pulls pairs directly from DexScreener's Solana endpoint
# and from GeckoTerminal Solana pools so Solana gets the
# same discovery breadth as EVM chains.
# =========================================================
async def scan_solana_dex(app: Application) -> None:
    """
    [FIX-12] Dedicated Solana scanner using DexScreener's chain-level
    pairs endpoint + GeckoTerminal Solana pools.
    Runs every 30s alongside scan_dexscreener so Solana tokens are
    never gated behind the Birdeye key.
    """
    while True:
        try:
            # --- DexScreener Solana pairs endpoint ---
            pairs = await fetch_dexscreener_solana_new_pairs()
            if DEBUG_MODE:
                log.debug(f'[SCAN-SOL] DexScreener solana_pairs: {len(pairs)} raw pairs')
            _process_pairs(app, pairs, 'solana')

            # --- GeckoTerminal Solana new pools ---
            gecko_pools = await fetch_gecko_new_pools('solana')
            for pool in gecko_pools:
                t = await gecko_pool_to_token(pool, 'solana')
                if not t or t.address in alerted_set:
                    continue
                # Enrich with DexScreener data where possible
                pair = await fetch_dexscreener_token(t.address, 'solana')
                if pair:
                    t2 = parse_dex_pair(pair, 'solana')
                    if t2:
                        t = t2
                asyncio.create_task(evaluate_and_alert(app, t))

        except Exception as e:
            log.error(f'[SCAN] scan_solana_dex error: {e}')

        await asyncio.sleep(DEXSCREENER_POLL_INTERVAL)


async def scan_birdeye(app: Application) -> None:
    while True:
        try:
            for tok in await fetch_birdeye_trending():
                address = tok.get('address')
                if not address or address in alerted_set:
                    continue
                pair = await fetch_dexscreener_token(address, 'solana')
                if not pair:
                    continue
                t = parse_dex_pair(pair, 'solana')
                if t and t.address not in alerted_set:
                    asyncio.create_task(evaluate_and_alert(app, t))

            for listing in await fetch_birdeye_new_listings():
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
            log.error(f'[SCAN] Birdeye error: {e}')

        await asyncio.sleep(BIRDEYE_POLL_INTERVAL)


async def scan_gecko_evm(app: Application) -> None:
    while True:
        for chain in ('base', 'ethereum'):
            try:
                pools = await fetch_gecko_new_pools(chain)
                for pool in pools:
                    t = await gecko_pool_to_token(pool, chain)
                    if not t or t.address in alerted_set:
                        continue
                    pair = await fetch_dexscreener_token(t.address, chain)
                    if pair:
                        t2 = parse_dex_pair(pair, chain)
                        if t2:
                            t = t2
                    asyncio.create_task(evaluate_and_alert(app, t))
            except Exception as e:
                log.error(f'[SCAN] GeckoTerminal error [{chain}]: {e}')
        await asyncio.sleep(60)


async def scan_raydium(app: Application) -> None:
    """[FIX-4] Scan Raydium new pools for Solana gems."""
    while True:
        try:
            mints = await fetch_raydium_new_pools()
            await _process_address_list(app, mints, 'solana')
        except Exception as e:
            log.error(f'[SCAN] Raydium error: {e}')
        await asyncio.sleep(60)


async def pumpfun_ws_loop(app: Application) -> None:
    if not PUMPPORTAL_KEY:
        log.info('[SCAN] PumpPortal key not set — skipping pump.fun WS')
        return

    uri = f'wss://pumpportal.fun/api/data?api-key={PUMPPORTAL_KEY}'
    retries = 0
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=30) as ws:
                retries = 0
                log.info('[SCAN] PumpFun WS connected')
                await ws.send(json.dumps({'method': 'subscribeNewToken'}))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        if msg.get('txType') != 'create' and not msg.get('raydiumPool'):
                            continue
                        mint = msg.get('mint', '').strip()
                        if not mint or mint in alerted_set:
                            continue
                        if DEBUG_MODE:
                            log.debug(f'[PUMP.FUN] Migration event: {mint}')
                        pair = await fetch_dexscreener_token(mint, 'solana')
                        if pair:
                            t = parse_dex_pair(pair, 'solana')
                            if t and t.address not in alerted_set:
                                asyncio.create_task(evaluate_and_alert(app, t))
                    except json.JSONDecodeError:
                        log.warning('[API-FAIL] PumpFun WS: malformed JSON')
                    except Exception as e:
                        log.debug(f'[PUMP.FUN] Event error: {e}')

        except Exception as e:
            retries += 1
            wait = min(10 * retries, 120)
            log.error(f'[SCAN] PumpFun WS error: {e} — retry in {wait}s')
            await asyncio.sleep(wait)


# =========================================================
# CA SCAN (manual lookup)
# =========================================================
async def scan_ca_manual(address: str) -> str:
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
            t.score = score; t.score_reasons = reasons

            ok, reason  = passes_hard_filters(t)
            mom_ok, mom_reason = passes_momentum_check(t)
            liq_ratio   = t.liquidity_usd / t.market_cap * 100 if t.market_cap else 0
            total_1h    = t.buys_1h + t.sells_1h
            bp_1h       = t.buys_1h / total_1h * 100 if total_1h else 0

            socials = []
            if t.twitter:       socials.append(f'[Twitter]({t.twitter})')
            if t.telegram_link: socials.append(f'[Telegram]({t.telegram_link})')
            if t.website:       socials.append(f'[Website]({t.website})')
            soc_str = ' · '.join(socials) if socials else 'None'

            sec_parts = []
            if t.has_mint_auth:    sec_parts.append('⚠️ Mint auth active')
            if t.has_freeze_auth:  sec_parts.append('⚠️ Freeze auth active')
            if t.is_honeypot:      sec_parts.append('🔴 HONEYPOT')
            if t.lp_locked:        sec_parts.append(f'✅ LP locked {t.lp_lock_pct:.0f}%')
            if t.rug_score:        sec_parts.append(f'Rug score: {t.rug_score}/100')
            if t.goplus_failed:    sec_parts.append('⚠️ GoPlus unavailable')
            if t.rugcheck_failed:  sec_parts.append('⚠️ Rugcheck unavailable')
            sec_str = ' · '.join(sec_parts) if sec_parts else 'No issues detected'

            top_pos = [r for r in reasons if r.startswith('+')][:4]
            neg     = [r for r in reasons if r.startswith('-')][:3]
            reasons_str = '\n'.join(f'  · {r.split(" ", 1)[1]}' for r in top_pos + neg)

            momentum_line = f'⏱ Momentum: {"✅ FRESH" if mom_ok else f"⚠️ LATE — {mom_reason}"}'

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
                f'{momentum_line}\n'
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

        expired = [addr for addr in alerted_set
                   if addr in gems and (time.time() - gems[addr].called_at) > ALERT_COOLDOWN_SEC]
        for addr in expired:
            alerted_set.discard(addr)

        if stale or expired:
            log.info(f'[CLEANUP] Removed {len(stale)} stale tokens, {len(expired)} cooldown entries')


async def log_stats() -> None:
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        alerted = sum(1 for t in gems.values() if t.called)
        log.info(
            f'[STATS] tracked={len(gems)} alerted={alerted} '
            f'watchlist={len(watchlist)} unique_alerted={len(alerted_set)} scans={scan_count}'
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
        '/status   — live scanner stats\n'
        '/calls    — this month\'s alerts\n'
        '/filters  — active filter settings\n'
        '/debug    — top candidates by score\n'
        '/watchlist — tokens being monitored\n'
        '/chains   — supported chains\n'
        '/forcecall <address> — force alert bypassing filters\n\n'
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
        f'Watchlist      : {len(watchlist)}\n'
        f'Cooldown pool  : {len(alerted_set)}\n'
        f'By chain       : {chain_str}\n\n'
    )
    if top:
        msg += '*Top candidates:*\n'
        for t in top:
            ok, r = passes_hard_filters(t)
            mom_ok, _ = passes_momentum_check(t)
            msg += (
                f'\n{chain_emoji(t.chain)} *{t.name}* (${t.symbol})\n'
                f'MC: {fmt(t.market_cap)} | Score: {t.score} | {t.heat_label}\n'
                f'{"✅ PASS" if ok else f"❌ {r}"} | Momentum: {"✅" if mom_ok else "⚠️ late"}\n'
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
        mom_ok, mom_reason = passes_momentum_check(t)
        total_1h   = t.buys_1h + t.sells_1h
        bp_1h      = t.buys_1h / total_1h * 100 if total_1h else 0
        lines.append(
            f'{chain_emoji(t.chain)} *{t.name}* | Score {t.score} | Age {age_str(t)}\n'
            f'MC={fmt(t.market_cap)} Liq={fmt(t.liquidity_usd)} BP={bp_1h:.0f}% Vol1h={fmt(t.vol_1h)}\n'
            f'{"✅ PASS" if ok else f"❌ {reason}"}\n'
            f'Momentum: {"✅ fresh" if mom_ok else f"⚠️ {mom_reason[:60]}"}\n'
            f'GoPlus: {"❌ failed" if t.goplus_failed else "✓"} | Rugcheck: {"❌ failed" if t.rugcheck_failed else "✓"}\n'
            f'_{" | ".join(r.split(" ", 1)[1] for r in t.score_reasons[:3] if r.startswith("+"))}_\n'
        )
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """[FIX-6] Show current watchlist."""
    if not watchlist:
        await update.message.reply_text('Watchlist is empty.')
        return
    lines = [f'*Watchlist — {len(watchlist)} tokens*\n']
    top   = sorted(watchlist.values(), key=lambda t: t.score, reverse=True)[:10]
    for t in top:
        age_wl = int(time.time() - t.watchlist_added) // 60
        lines.append(
            f'{chain_emoji(t.chain)} *{t.name}* (${t.symbol})\n'
            f'Score: {t.score} | MC: {fmt(t.market_cap)} | '
            f'Vol1h: {fmt(t.vol_1h)} | In WL: {age_wl}m | Rescans: {t.watchlist_rescans}\n'
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
        '*Active Filters* _(relaxed — calibration mode)_\n\n'
        f'MC Range            : {fmt(MC_MIN)} – {fmt(MC_MAX)}\n'
        f'Liq/MC Ratio Min    : {LIQ_MC_RATIO_MIN:.0%}\n'
        f'Min Vol 1h          : {fmt(VOL_1H_MIN)}\n'
        f'Min Vol 5m          : {fmt(VOL_5M_MIN)}\n'
        f'Min Buy Pressure    : {BUY_PRESSURE_MIN}%\n'
        f'Min Holders         : {HOLDER_MIN}\n'
        f'Max Top Holder %    : {MAX_TOP_HOLDER_PCT}%\n'
        f'Require Socials     : Waived if score ≥ {SOCIALS_WAIVER_SCORE}\n'
        f'Migration Only      : {"Yes" if MIGRATION_ONLY else "No"}\n'
        f'Migration Max Age   : {MIGRATION_MAX_HOURS}h\n'
        f'Min Score (Alert)   : {MIN_SCORE}/100\n'
        f'Min Score (Watchlist): {WATCHLIST_MIN_SCORE}/100\n'
        f'Alert Cooldown      : {ALERT_COOLDOWN_SEC//3600}h\n'
        f'Debug Mode          : {"ON" if DEBUG_MODE else "OFF"}\n\n'
        f'Momentum Gate       : 1h>{MOMENTUM_LATE_PUMP_1H_THRESHOLD:.0f}% + 5m<{MOMENTUM_LATE_PUMP_5M_MIN}% → suppressed\n'
        f'Milestone Gate      : BP≥{MILESTONE_BUY_PRESSURE_MIN}% + vol_ratio≥{MILESTONE_VOL_DECAY_RATIO}\n\n'
        f'Chains: {", ".join(chain_name(c) for c in CHAINS)}',
        parse_mode='Markdown',
    )


async def cmd_chains(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lines = ['*Supported Chains*\n']
    for c, info in CHAINS.items():
        lines.append(f'{info["emoji"]} {info["name"]}')
    await update.message.reply_text('\n'.join(lines), parse_mode='Markdown')


async def cmd_forcecall(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    [FIX-9] /forcecall <address> — bypass all filters, show raw metrics + send alert.
    For live debugging only.
    """
    args = context.args
    if not args:
        await update.message.reply_text(
            'Usage: /forcecall <token_address>\n\n'
            'Bypasses all filters. For debugging only.'
        )
        return

    address = args[0].strip()
    msg = await update.message.reply_text(f'🔍 Force scanning `{address[:16]}...`', parse_mode='Markdown')

    found = False
    for chain in CHAINS:
        pair = await fetch_dexscreener_token(address, chain)
        if not pair:
            continue
        found = True
        t = parse_dex_pair(pair, chain)
        if not t:
            continue

        await run_security_checks(t)
        if chain == 'solana':
            await enrich_from_birdeye(t)

        score, reasons = compute_score(t)
        t.score         = score
        t.score_reasons = reasons
        t.caution_label = get_caution_label(t)
        t.heat_label    = get_heat_label(t)

        ok, filter_reason  = passes_hard_filters(t)
        mom_ok, mom_reason = passes_momentum_check(t)
        liq_ratio          = t.liquidity_usd / t.market_cap * 100 if t.market_cap else 0
        total_1h           = t.buys_1h + t.sells_1h
        bp_1h              = t.buys_1h / total_1h * 100 if total_1h else 0

        pos_reasons = [r for r in reasons if r.startswith('+')]
        neg_reasons = [r for r in reasons if r.startswith('-')]

        debug_text = (
            f'🔧 *FORCE CALL DEBUG*\n\n'
            f'{chain_emoji(chain)} *{t.name}* (${t.symbol}) · {chain_name(chain)}\n'
            f'Age: {age_str(t)}\n\n'
            f'*Raw Metrics:*\n'
            f'MC: {fmt(t.market_cap)}\n'
            f'Liq: {fmt(t.liquidity_usd)} ({liq_ratio:.1f}% of MC)\n'
            f'Vol 5m: {fmt(t.vol_5m)} | Vol 1h: {fmt(t.vol_1h)}\n'
            f'Buy Pressure 1h: {bp_1h:.0f}% ({t.buys_1h}B / {t.sells_1h}S)\n'
            f'Price: {fmt_pct(t.price_change_5m)} 5m · {fmt_pct(t.price_change_1h)} 1h\n'
            f'Holders: {t.holder_count:,}\n\n'
            f'*Security:*\n'
            f'Honeypot: {t.is_honeypot} | Mint Auth: {t.has_mint_auth} | Freeze: {t.has_freeze_auth}\n'
            f'Rug Score: {t.rug_score}/100 | LP Locked: {t.lp_locked} ({t.lp_lock_pct:.0f}%)\n'
            f'GoPlus: {"❌ failed" if t.goplus_failed else "✓"} | Rugcheck: {"❌ failed" if t.rugcheck_failed else "✓"}\n\n'
            f'*Score: {score}/100*\n'
            f'Filter: {"✅ PASS" if ok else f"❌ {filter_reason}"}\n'
            f'Momentum: {"✅ FRESH" if mom_ok else f"⚠️ {mom_reason}"}\n\n'
            f'*Scoring (+):*\n' +
            '\n'.join(f'  {r}' for r in pos_reasons) +
            ('\n\n*Scoring (-):*\n' + '\n'.join(f'  {r}' for r in neg_reasons) if neg_reasons else '') +
            f'\n\n`{address}`'
        )

        try:
            await msg.edit_text(debug_text, parse_mode='Markdown', disable_web_page_preview=True)
        except Exception:
            await msg.edit_text(debug_text, disable_web_page_preview=True)

        # Force send the actual alert regardless of filters
        if t.address not in alerted_set:
            gems[t.address] = t
            await send_alert(update.get_bot() if hasattr(update, 'get_bot') else None, t)
            # Use app bot directly
            try:
                dex_url = t.dex_url or f'https://dexscreener.com/{t.chain}/{t.address}'
                buttons = [[InlineKeyboardButton('📊 DexScreener', url=dex_url)]]
                if t.chain == 'solana':
                    buttons[0].append(InlineKeyboardButton('⚡ Photon', url=f'https://photon-sol.tinyastro.io/en/lp/{t.address}'))
                await update.message.reply_text(
                    build_alert(t),
                    parse_mode='Markdown',
                    disable_web_page_preview=True,
                    reply_markup=InlineKeyboardMarkup(buttons),
                )
                alerted_set.add(t.address)
                t.called    = True
                t.called_at = time.time()
                t.entry_mc  = t.market_cap
                log.info(f'[FORCECALL] Alert sent for {t.name} ({t.symbol})')
            except Exception as e:
                log.error(f'[FORCECALL] Alert error: {e}')
        return

    if not found:
        try:
            await msg.edit_text(
                f'❌ No data found for `{address}`\n\n'
                f'Token not on DexScreener yet.\n'
                f'[Check manually](https://dexscreener.com/solana/{address})',
                parse_mode='Markdown',
                disable_web_page_preview=True,
            )
        except Exception:
            pass


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text   = (update.message.text or '').strip()
    SOL_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')
    EVM_RE = re.compile(r'\b0x[0-9a-fA-F]{40}\b')

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
            f'GemStalker v2 OK | tracked={len(gems)} alerted={alerted} watchlist={len(watchlist)}'.encode()
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
    asyncio.create_task(scan_birdeye(app))           # Birdeye (requires key; supplemental for Solana)
    asyncio.create_task(scan_solana_dex(app))        # [FIX-12] Dedicated Solana — always runs
    asyncio.create_task(scan_gecko_evm(app))
    asyncio.create_task(scan_raydium(app))           # [FIX-4]
    asyncio.create_task(pumpfun_ws_loop(app))
    asyncio.create_task(rescan_watchlist(app))       # [FIX-6]
    asyncio.create_task(cleanup_loop())
    asyncio.create_task(log_stats())
    log.info(
        f'GemStalker v2 ready | '
        f'chains={list(CHAINS.keys())} | '
        f'min_score={MIN_SCORE} | watchlist_min={WATCHLIST_MIN_SCORE} | '
        f'debug={"ON" if DEBUG_MODE else "OFF"} | '
        f'birdeye={"✓" if BIRDEYE_KEY else "✗"} | '
        f'goplus={"✓" if GOPLUS_KEY else "✗"} | '
        f'solana_scanner=always_on'
    )


def main() -> None:
    if not TG_TOKEN: raise RuntimeError('TELEGRAM_BOT_TOKEN not set')
    if not CHAT_ID:  raise RuntimeError('CHAT_ID not set')
    if not BIRDEYE_KEY:
        log.warning('BIRDEYE_API_KEY not set — Birdeye scanning disabled (scan_solana_dex still active)')
    if not GOPLUS_KEY:
        log.warning('GOPLUS_API_KEY not set — GoPlus security checks limited')
    if not PUMPPORTAL_KEY:
        log.warning('PUMPPORTAL_API_KEY not set — pump.fun WS disabled')

    log.info(f'[STARTUP] Debug mode: {"ON" if DEBUG_MODE else "OFF"}')
    log.info(f'[STARTUP] Filters: MC={fmt(MC_MIN)}-{fmt(MC_MAX)} Liq/MC≥{LIQ_MC_RATIO_MIN:.0%} Score≥{MIN_SCORE}')
    log.info(f'[STARTUP] Momentum gate: 1h>{MOMENTUM_LATE_PUMP_1H_THRESHOLD}% + 5m<{MOMENTUM_LATE_PUMP_5M_MIN}% → suppressed')

    threading.Thread(target=run_health, daemon=True).start()

    app = Application.builder().token(TG_TOKEN).build()
    app.add_handler(CommandHandler('start',     cmd_start))
    app.add_handler(CommandHandler('status',    cmd_status))
    app.add_handler(CommandHandler('calls',     cmd_calls))
    app.add_handler(CommandHandler('filters',   cmd_filters))
    app.add_handler(CommandHandler('debug',     cmd_debug))
    app.add_handler(CommandHandler('chains',    cmd_chains))
    app.add_handler(CommandHandler('watchlist', cmd_watchlist))   # [FIX-6]
    app.add_handler(CommandHandler('forcecall', cmd_forcecall))   # [FIX-9]
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.post_init = post_init

    log.info('GemStalker v2 starting...')
    app.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()
