"""
Nifty Master Algo V8  —  fetch-snapshot.py
===========================================
Designed for CRON execution. One run = one snapshot cycle, then exits.
Schedule via cron: */3 9-15 * * 1-5  python /path/to/fetch-snapshot.py

After computing all signals this script:
  1. Prints full snapshot display to console
  2. Writes latest-calculations.txt  (overwritten each run)
  3. Calls nifty_notify.py           (Telegram + Gemini AI analysis)
  4. Exits cleanly

No scheduler loop. No backtest engine. No Teams notifications.
"""

import sqlite3
import time
import datetime
import os
import sys
import subprocess
import pandas as pd
import numpy as np
import requests
import logging
from zoneinfo import ZoneInfo
from logging.handlers import RotatingFileHandler

# ==========================================
# SYSTEM & LOGGING SETUP
# ==========================================
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        pass

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        RotatingFileHandler(
            "nifty_algo_v8.log", maxBytes=5 * 1024 * 1024,
            backupCount=2, encoding='utf-8'
        ),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

IST = ZoneInfo('Asia/Kolkata')

# ==========================================
# CONFIGURATION & CONSTANTS
# ==========================================
DB_NAME                = "nifty_algo_v8.db"
FETCH_INTERVAL_MINUTES = 3
STRIKE_WINDOW          = 250
MIN_OI_CHANGE_THRESH   = 10000
LOT_SIZE               = 50

# Portfolio & Risk Settings
CAPITAL            = 100000
RISK_PER_TRADE_PCT = 0.01
MAX_DAILY_LOSS     = -0.02 * CAPITAL   # Rs -2,000 daily stop

# Premium-based exit controls
MAX_PREMIUM_LOSS_PCT  = 0.40
TRAIL_LOCK_TRIGGER    = 0.30
TRAIL_LOCK_PCT        = 0.50
EXPIRY_TIME_GUARD_MIN = 30

# Higher Timeframe levels — update daily before market open
PREV_DAY_HIGH  = 22500.0
PREV_DAY_LOW   = 22100.0
PREV_DAY_CLOSE = 22350.0

# Output file read by nifty_notify.py
LATEST_CALC_FILE = "latest-calculations.txt"

# ==========================================
# NSE HOLIDAYS
# ==========================================
NSE_HOLIDAYS_2025 = [
    "2025-01-26", "2025-02-26", "2025-03-14", "2025-04-10",
    "2025-04-14", "2025-04-18", "2025-05-01", "2025-08-15",
    "2025-08-27", "2025-10-02", "2025-10-21", "2025-10-22",
    "2025-11-05", "2025-12-25",
]
NSE_HOLIDAYS_2026 = [
    "2026-01-26", "2026-03-20", "2026-04-01", "2026-04-011",
    "2026-04-14", "2026-08-15", "2026-10-02", "2026-11-09",
    "2026-12-25",
]
NSE_HOLIDAYS: set = set(NSE_HOLIDAYS_2025 + NSE_HOLIDAYS_2026)

# ==========================================
# REQUESTS SESSION (module-level, shared)
# ==========================================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json, text/plain, */*",
    "Referer":    "https://www.nseindia.com/option-chain",
})


def fetch_and_cache_nse_holidays() -> None:
    """Refresh holiday list from NSE API; silently falls back to hardcoded set."""
    global NSE_HOLIDAYS
    try:
        url  = "https://www.nseindia.com/api/holiday-master?type=trading"
        resp = session.get(url, timeout=10)
        if resp.ok:
            raw     = resp.json().get("CM", [])
            fetched = set()
            for entry in raw:
                raw_date = entry.get("tradingDate", "")
                try:
                    dt = datetime.datetime.strptime(raw_date, "%d-%b-%Y")
                    fetched.add(dt.strftime("%Y-%m-%d"))
                except ValueError:
                    continue
            if fetched:
                NSE_HOLIDAYS = fetched
                logger.info(f"NSE holidays loaded from API: {len(NSE_HOLIDAYS)} dates")
                return
    except Exception as e:
        logger.warning(f"NSE holiday API fetch failed: {e}. Using hardcoded list.")
    logger.info(f"Using hardcoded NSE holiday list: {len(NSE_HOLIDAYS)} dates")


# ==========================================
# GLOBAL TRADE STATE
# Re-hydrated from DB at the start of each cron run
# ==========================================
ACTIVE_TRADE         = None
TRADES_TODAY         = 0
DAILY_PNL            = 0.0
LAST_TRADE_TIME      = None
CURRENT_TRADING_DAY  = None
LAST_10_TRADES: list = []


def load_trade_state_from_db() -> None:
    """
    Re-hydrates all global trade-state variables from the DB.
    Required because the process exits after every cron run — there
    are no in-memory globals that survive between runs.
    """
    global ACTIVE_TRADE, TRADES_TODAY, DAILY_PNL, LAST_TRADE_TIME
    global CURRENT_TRADING_DAY, LAST_10_TRADES

    today = datetime.datetime.now(IST).date()
    CURRENT_TRADING_DAY = today

    try:
        with sqlite3.connect(DB_NAME) as conn:
            # Today's closed trades
            cur = conn.execute(
                """SELECT COUNT(*), COALESCE(SUM(pnl), 0)
                   FROM trade_log
                   WHERE action='EXIT' AND date(timestamp)=?""",
                (today.strftime("%Y-%m-%d"),)
            )
            row          = cur.fetchone()
            TRADES_TODAY = int(row[0])   if row else 0
            DAILY_PNL    = float(row[1]) if row else 0.0

            # Last 10 trades for performance_guard
            cur2 = conn.execute(
                """SELECT pnl FROM trade_log
                   WHERE action='EXIT'
                   ORDER BY id DESC LIMIT 10"""
            )
            LAST_10_TRADES = [1 if r[0] > 0 else 0 for r in cur2.fetchall()]

            # Most recent ENTRY — check if it has a matching EXIT
            cur3 = conn.execute(
                """SELECT * FROM trade_log
                   WHERE action='ENTRY'
                   ORDER BY id DESC LIMIT 1"""
            )
            entry_row = cur3.fetchone()
            if entry_row:
                cols  = [d[0] for d in cur3.description]
                entry = dict(zip(cols, entry_row))
                cur4  = conn.execute(
                    "SELECT id FROM trade_log WHERE action='EXIT' AND id > ?",
                    (entry["id"],)
                )
                if cur4.fetchone() is None:
                    # Entry has no subsequent exit — trade is still open
                    ACTIVE_TRADE = {
                        "type":               entry["type"],
                        "entry_time":         datetime.datetime.strptime(
                                                  entry["timestamp"], "%Y-%m-%d %H:%M:%S"
                                              ).replace(tzinfo=IST),
                        "entry_spot":         entry.get("entry_spot", 0),
                        "option_strike":      entry.get("option_strike", 0),
                        "option_entry_ltp":   entry.get("entry_option_ltp", 0),
                        "peak_option_ltp":    entry.get("entry_option_ltp", 0),
                        "premium_trail_stop": None,
                        "stop_loss":          entry.get("entry_spot", 0),
                        "lots":               entry.get("lots", 1),
                    }
                    LAST_TRADE_TIME = ACTIVE_TRADE["entry_time"]

            logger.info(
                f"State: trades_today={TRADES_TODAY} daily_pnl=Rs{DAILY_PNL:.2f} "
                f"active={'YES' if ACTIVE_TRADE else 'NO'}"
            )

    except Exception as e:
        logger.warning(f"Could not load trade state from DB: {e}")


# ==========================================
# 1. DATABASE SETUP
# ==========================================
def init_db() -> None:
    with sqlite3.connect(DB_NAME) as conn:
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS raw_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   DATETIME,
                spot_price  REAL,
                strike      REAL,
                call_oi     REAL,  put_oi     REAL,
                ce_bid      REAL,  ce_ask     REAL,  ce_ltp REAL, ce_vol INTEGER,
                pe_bid      REAL,  pe_ask     REAL,  pe_ltp REAL, pe_vol INTEGER,
                UNIQUE(timestamp, strike)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS trade_log (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp         DATETIME,
                action            TEXT,
                type              TEXT,
                option_strike     REAL,
                entry_spot        REAL,
                exit_spot         REAL,
                entry_option_ltp  REAL,
                exit_option_ltp   REAL,
                pnl               REAL,
                lots              INTEGER,
                probability       REAL,
                notes             TEXT
            )
        ''')
        conn.commit()


# ==========================================
# 2. NSE FETCHER
# ==========================================
def fetch_nifty_data():
    """Returns (chain_json, nearest_expiry_str) or (None, None) on failure."""
    symbol   = "NIFTY"
    base_url = "https://www.nseindia.com"

    try:
        session.get(base_url, timeout=10)
        time.sleep(1)

        exp_resp = session.get(
            f"{base_url}/api/option-chain-contract-info?symbol={symbol}", timeout=10
        )
        if not exp_resp.ok:
            return None, None

        nearest_expiry = exp_resp.json()['expiryDates'][0]

        chain_resp = session.get(
            f"{base_url}/api/option-chain-v3"
            f"?type=Indices&symbol={symbol}&expiry={nearest_expiry}",
            timeout=10
        )
        if not chain_resp.ok:
            return None, None

        return chain_resp.json(), nearest_expiry

    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return None, None


def store_snapshot(data) -> datetime.datetime:
    """Writes one row per strike to raw_snapshots. Prunes rows older than 6 h."""
    if not data or 'records' not in data:
        return None

    spot      = data['records']['underlyingValue']
    records   = data['records']['data']
    timestamp = datetime.datetime.now(IST).replace(second=0, microsecond=0)

    with sqlite3.connect(DB_NAME, isolation_level='IMMEDIATE') as conn:
        c      = conn.cursor()
        cutoff = timestamp - datetime.timedelta(hours=6)
        c.execute(
            "DELETE FROM raw_snapshots WHERE timestamp < ?",
            (cutoff.strftime('%Y-%m-%d %H:%M:%S'),)
        )
        insert_q = '''
            INSERT OR IGNORE INTO raw_snapshots
            (timestamp, spot_price, strike,
             call_oi, put_oi,
             ce_bid, ce_ask, ce_ltp, ce_vol,
             pe_bid, pe_ask, pe_ltp, pe_vol)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        for r in records:
            ce = r.get('CE', {})
            pe = r.get('PE', {})
            c.execute(insert_q, (
                timestamp.strftime('%Y-%m-%d %H:%M:%S'), spot, r.get('strikePrice'),
                ce.get('openInterest', 0),  pe.get('openInterest', 0),
                ce.get('bidprice', 0),      ce.get('askPrice', 0),
                ce.get('lastPrice', 0),     ce.get('totalTradedVolume', 0),
                pe.get('bidprice', 0),      pe.get('askPrice', 0),
                pe.get('lastPrice', 0),     pe.get('totalTradedVolume', 0),
            ))
        conn.commit()

    return timestamp


# ==========================================
# 3. CONTEXT ENGINES
# ==========================================

def get_recent_candle_spots(df_all: pd.DataFrame, n_candles: int) -> pd.DataFrame:
    """One row per candle sorted ASC (oldest first), last n_candles only."""
    per_ts = (
        df_all
        .groupby('timestamp')['spot_price']
        .first()
        .sort_index(ascending=True)
        .tail(n_candles)
        .reset_index()
    )
    per_ts.columns = ['timestamp', 'spot_price']
    return per_ts


def calculate_session_vwap(df_today: pd.DataFrame):
    """Session VWAP using CE+PE volume as participation weight. Returns float|None."""
    if df_today.empty:
        return None

    per_ts = (
        df_today
        .groupby('timestamp')
        .agg(spot=('spot_price', 'first'))
        .sort_index(ascending=True)
        .reset_index()
    )
    ce_vol = df_today.groupby('timestamp')['ce_vol'].sum().sort_index().values
    pe_vol = df_today.groupby('timestamp')['pe_vol'].sum().sort_index().values
    per_ts['volume'] = ce_vol + pe_vol

    total_vol = per_ts['volume'].sum()
    if total_vol == 0:
        return float(per_ts['spot'].mean())

    per_ts['pv']      = per_ts['spot'] * per_ts['volume']
    per_ts['cum_pv']  = per_ts['pv'].cumsum()
    per_ts['cum_vol'] = per_ts['volume'].cumsum()
    per_ts['vwap']    = per_ts['cum_pv'] / per_ts['cum_vol']

    return float(per_ts['vwap'].iloc[-1])


# ── Regime detection constants ──────────────────────────────────────────────
REGIME_TREND_MIN_DIRECTION   = 50
REGIME_TREND_DIRECTION_RATIO = 0.60
REGIME_RANGE_MAX_RANGE       = 30
REGIME_OI_DOMINANCE_RATIO    = 2.0
REGIME_OI_BOTH_SURGE_THRESH  = 50000
REGIME_VOL_INCREASE_RATIO    = 1.20


def _pillar_price_structure(spots: list) -> dict:
    high          = max(spots)
    low           = min(spots)
    range_pts     = high - low
    direction_pts = spots[-1] - spots[0]
    abs_dir       = abs(direction_pts)
    efficiency    = (abs_dir / range_pts) if range_pts > 0 else 0.0

    if abs_dir >= REGIME_TREND_MIN_DIRECTION and efficiency >= REGIME_TREND_DIRECTION_RATIO:
        signal = "TREND_UP" if direction_pts > 0 else "TREND_DOWN"
    elif range_pts <= REGIME_RANGE_MAX_RANGE:
        signal = "RANGE"
    else:
        signal = "VOLATILE"

    return {
        "direction_pts": direction_pts,
        "range_pts":     range_pts,
        "efficiency":    round(efficiency, 3),
        "signal":        signal,
    }


def _pillar_oi_dominance(df_near: pd.DataFrame) -> dict:
    if df_near.empty or 'delta_call' not in df_near.columns:
        return {"net_call_change": 0, "net_put_change": 0,
                "bias": "NEUTRAL", "both_surging": False}

    net_call = float(df_near['delta_call'].sum())
    net_put  = float(df_near['delta_put'].sum())

    both_surging = (
        net_call > REGIME_OI_BOTH_SURGE_THRESH and
        net_put  > REGIME_OI_BOTH_SURGE_THRESH
    )

    if both_surging:
        bias = "VOLATILE"
    else:
        abs_call = abs(net_call)
        abs_put  = abs(net_put)

        if abs_put > 0 and abs_call > 0:
            dom_ratio = max(abs_call, abs_put) / min(abs_call, abs_put)
        else:
            dom_ratio = REGIME_OI_DOMINANCE_RATIO + 1

        if dom_ratio < REGIME_OI_DOMINANCE_RATIO:
            bias = "NEUTRAL"
        else:
            bullish_oi = (net_put > 0 and abs_put >= abs_call) or \
                         (net_call < 0 and abs_call >= abs_put)
            bearish_oi = (net_call > 0 and abs_call >= abs_put) or \
                         (net_put < 0 and abs_put >= abs_call)

            if bullish_oi and not bearish_oi:
                bias = "BULLISH"
            elif bearish_oi and not bullish_oi:
                bias = "BEARISH"
            else:
                bias = "NEUTRAL"

    return {
        "net_call_change": net_call,
        "net_put_change":  net_put,
        "bias":            bias,
        "both_surging":    both_surging,
    }


def _pillar_participation(df_all: pd.DataFrame, n_candles: int = 10) -> dict:
    per_ts = (
        df_all
        .groupby('timestamp')
        .agg(total_vol=pd.NamedAgg(column='ce_vol', aggfunc='sum'))
        .sort_index(ascending=True)
    )
    pe_per_ts = df_all.groupby('timestamp')['pe_vol'].sum().sort_index()
    per_ts['total_vol'] = per_ts['total_vol'] + pe_per_ts

    if len(per_ts) < 2:
        return {"current_vol": 0, "avg_prior_vol": 0,
                "vol_ratio": 1.0, "participation": "NEUTRAL"}

    recent    = per_ts.tail(n_candles)
    current_v = float(recent['total_vol'].iloc[-1])
    prior_v   = float(recent['total_vol'].iloc[:-1].mean()) if len(recent) > 1 else current_v
    ratio     = (current_v / prior_v) if prior_v > 0 else 1.0

    participation = (
        "STRONG" if ratio >= REGIME_VOL_INCREASE_RATIO else
        "WEAK"   if ratio <= (1.0 / REGIME_VOL_INCREASE_RATIO) else
        "NEUTRAL"
    )
    return {
        "current_vol":   current_v,
        "avg_prior_vol": round(prior_v, 0),
        "vol_ratio":     round(ratio, 3),
        "participation": participation,
    }


def detect_market_regime(
    df_recent_sorted: pd.DataFrame,
    df_near: pd.DataFrame,
    df_all: pd.DataFrame,
) -> dict:
    """3-Pillar Regime Engine. Returns dict with 'label' + pillar sub-dicts."""
    spots = df_recent_sorted['spot_price'].tolist()
    if len(spots) < 3:
        return {"label": "VOLATILE", "price": {}, "oi": {}, "volume": {}}

    p1 = _pillar_price_structure(spots)
    p2 = _pillar_oi_dominance(df_near)
    p3 = _pillar_participation(df_all)

    price_sig     = p1["signal"]
    oi_bias       = p2["bias"]
    participation = p3["participation"]

    if price_sig == "VOLATILE" or oi_bias == "VOLATILE" or p2["both_surging"]:
        label = "VOLATILE"
    elif price_sig == "RANGE" and oi_bias == "NEUTRAL":
        label = "RANGE"
    elif price_sig == "TREND_UP" and oi_bias == "BULLISH" and participation == "STRONG":
        label = "TREND_UP"
    elif price_sig == "TREND_DOWN" and oi_bias == "BEARISH" and participation == "STRONG":
        label = "TREND_DOWN"
    elif price_sig == "TREND_UP" and oi_bias == "BULLISH":
        label = "WEAK_TREND_UP"
    elif price_sig == "TREND_DOWN" and oi_bias == "BEARISH":
        label = "WEAK_TREND_DOWN"
    elif price_sig in ("TREND_UP", "TREND_DOWN") and oi_bias == "NEUTRAL":
        label = "VOLATILE"
    elif price_sig == "RANGE" and oi_bias in ("BULLISH", "BEARISH"):
        label = "RANGE"
    else:
        label = "VOLATILE"

    return {"label": label, "price": p1, "oi": p2, "volume": p3}


def calculate_momentum(df_recent_sorted: pd.DataFrame) -> str:
    spots = df_recent_sorted['spot_price'].tolist()
    if len(spots) < 3:
        return "WEAK"
    momentum = spots[-1] - spots[-3]
    if abs(momentum) < 10:
        return "WEAK"
    return "STRONG_UP" if momentum > 0 else "STRONG_DOWN"


def get_higher_tf_bias(spot: float) -> str:
    if spot > PREV_DAY_HIGH:
        return "BULLISH"
    if spot < PREV_DAY_LOW:
        return "BEARISH"
    return "RANGE"


# ==========================================
# 4. EXECUTION ENGINES
# ==========================================

def validate_execution_and_liquidity(df_t0, spot, spot_t1, trade_type):
    if abs(spot - spot_t1) > 20:
        logger.warning("Spike > 20 pts. Delaying entry.")
        return False, None, None

    atm           = round(spot / 50) * 50
    target_strike = atm - 50 if trade_type == "CALL" else atm + 50

    opt_data = df_t0[df_t0['strike'] == target_strike]
    if opt_data.empty:
        return False, None, None

    rec    = opt_data.iloc[0]
    bid    = rec['ce_bid'] if trade_type == "CALL" else rec['pe_bid']
    ask    = rec['ce_ask'] if trade_type == "CALL" else rec['pe_ask']
    vol    = rec['ce_vol'] if trade_type == "CALL" else rec['pe_vol']
    ltp    = rec['ce_ltp'] if trade_type == "CALL" else rec['pe_ltp']
    spread = ask - bid

    if spread > 5.0 or vol < 1000:
        logger.warning(f"Bad liquidity at {target_strike}. Spread:{spread:.2f} Vol:{vol}")
        return False, None, None

    return True, target_strike, ltp


def calculate_position_size(option_ltp: float) -> int:
    risk_per_trade = CAPITAL * RISK_PER_TRADE_PCT
    risk_per_lot   = max(option_ltp * 0.30 * LOT_SIZE, 1)
    return max(1, int(np.floor(risk_per_trade / risk_per_lot)))


def performance_guard() -> float:
    if len(LAST_10_TRADES) >= 5:
        win_rate = sum(LAST_10_TRADES[-10:]) / len(LAST_10_TRADES[-10:])
        if win_rate < 0.40:
            logger.warning(f"Win rate {win_rate*100:.1f}%. Halving size.")
            return 0.5
    return 1.0


# ==========================================
# 5. CORE LOGIC & EXITS
# ==========================================

def evaluate_entry(signal: dict) -> str:
    regime      = signal['regime']
    trap_status = signal['trap_status']

    if regime in ("VOLATILE", "RANGE") or trap_status != "None":
        return "NO_TRADE"

    min_score = 2 if regime in ("WEAK_TREND_UP", "WEAK_TREND_DOWN") else 1

    if regime in ("TREND_UP", "WEAK_TREND_UP"):
        if signal['bullish_score'] >= min_score and signal['spot'] > signal['support']:
            if signal['spot_t1'] > signal['support']:
                return "CALL"

    if regime in ("TREND_DOWN", "WEAK_TREND_DOWN"):
        if signal['bearish_score'] >= min_score and signal['spot'] < signal['resistance']:
            if signal['spot_t1'] < signal['resistance']:
                return "PUT"

    return "NO_TRADE"


def get_current_option_ltp(df_t0: pd.DataFrame, trade: dict):
    row = df_t0[df_t0['strike'] == trade['option_strike']]
    if row.empty:
        return None
    rec = row.iloc[0]
    return float(rec['ce_ltp'] if trade['type'] == "CALL" else rec['pe_ltp'])


def enhanced_exit_logic(
    open_trade: dict,
    latest_signal: dict,
    current_opt_ltp: float,
    expiry_date_str: str,
) -> str:
    spot        = latest_signal['spot']
    vwap        = latest_signal['vwap']
    momentum    = latest_signal['momentum']
    now_ts      = latest_signal['timestamp']
    time_in_min = (now_ts - open_trade['entry_time']).total_seconds() / 60
    entry_ltp   = open_trade['option_entry_ltp']

    if current_opt_ltp <= entry_ltp * (1.0 - MAX_PREMIUM_LOSS_PCT):
        return f"FULL_EXIT (Premium SL {MAX_PREMIUM_LOSS_PCT*100:.0f}%)"

    if current_opt_ltp > open_trade['peak_option_ltp']:
        open_trade['peak_option_ltp'] = current_opt_ltp
        gain_pct = (current_opt_ltp - entry_ltp) / entry_ltp
        if gain_pct >= TRAIL_LOCK_TRIGGER:
            locked = (open_trade['peak_option_ltp'] - entry_ltp) * TRAIL_LOCK_PCT
            open_trade['premium_trail_stop'] = entry_ltp + locked

    if open_trade.get('premium_trail_stop') is not None:
        if current_opt_ltp < open_trade['premium_trail_stop']:
            return "FULL_EXIT (Premium Trail Stop)"

    if open_trade['type'] == "CALL" and spot < latest_signal['support']:
        return "FULL_EXIT (Structure Break)"
    if open_trade['type'] == "PUT"  and spot > latest_signal['resistance']:
        return "FULL_EXIT (Structure Break)"

    if vwap is not None:
        if open_trade['type'] == "CALL" and spot < vwap:
            return "FULL_EXIT (VWAP Cross Down)"
        if open_trade['type'] == "PUT"  and spot > vwap:
            return "FULL_EXIT (VWAP Cross Up)"

    if open_trade['type'] == "CALL" and momentum == "STRONG_DOWN":
        return "FULL_EXIT (Momentum Reversal)"
    if open_trade['type'] == "PUT"  and momentum == "STRONG_UP":
        return "FULL_EXIT (Momentum Reversal)"

    if time_in_min > 15:
        return "FULL_EXIT (Time Stop)"

    try:
        expiry_close = datetime.datetime.strptime(
            expiry_date_str, "%d-%b-%Y"
        ).replace(hour=15, minute=30, tzinfo=IST)
        mins_to_exp = (expiry_close - now_ts).total_seconds() / 60
        if mins_to_exp < EXPIRY_TIME_GUARD_MIN:
            if current_opt_ltp < open_trade['peak_option_ltp'] * 0.80:
                return "FULL_EXIT (Expiry Time Guard)"
    except ValueError:
        pass

    if open_trade['type'] == "CALL" and spot <= open_trade['stop_loss']:
        return "FULL_EXIT (Spot Trailing SL)"
    if open_trade['type'] == "PUT"  and spot >= open_trade['stop_loss']:
        return "FULL_EXIT (Spot Trailing SL)"

    if open_trade['type'] == "CALL" and spot > open_trade['stop_loss'] + 25:
        open_trade['stop_loss'] = spot - 20
    if open_trade['type'] == "PUT"  and spot < open_trade['stop_loss'] - 25:
        open_trade['stop_loss'] = spot + 20

    return "HOLD"


def final_trade_filter(signal: dict) -> bool:
    conditions_met = 0
    regime         = signal['regime']
    regime_data    = signal.get('regime_data', {})
    oi_sub         = regime_data.get('oi', {})
    vol_sub        = regime_data.get('volume', {})
    trade_type     = signal['type_considered']

    if trade_type == "CALL" and regime in ("TREND_UP", "WEAK_TREND_UP"):
        conditions_met += 1
    elif trade_type == "PUT" and regime in ("TREND_DOWN", "WEAK_TREND_DOWN"):
        conditions_met += 1

    if signal['vwap'] is not None:
        if (trade_type == "CALL" and signal['spot'] > signal['vwap']) or \
           (trade_type == "PUT"  and signal['spot'] < signal['vwap']):
            conditions_met += 1

    if (trade_type == "CALL" and signal['momentum'] == "STRONG_UP") or \
       (trade_type == "PUT"  and signal['momentum'] == "STRONG_DOWN"):
        conditions_met += 1

    if signal['trap_status'] == "None":
        conditions_met += 1

    if (trade_type == "CALL" and signal['htf_bias'] == "BULLISH") or \
       (trade_type == "PUT"  and signal['htf_bias'] == "BEARISH"):
        conditions_met += 1

    oi_bias = oi_sub.get('bias', 'NEUTRAL')
    if (trade_type == "CALL" and oi_bias == "BULLISH") or \
       (trade_type == "PUT"  and oi_bias == "BEARISH"):
        conditions_met += 1

    required = 3 if (
        regime in ("TREND_UP", "TREND_DOWN") and
        vol_sub.get('participation') == "STRONG"
    ) else 4

    return conditions_met >= required


def calculate_pnl(trade: dict, exit_opt_ltp: float) -> float:
    return (exit_opt_ltp - trade['option_entry_ltp']) * LOT_SIZE * trade['lots']


# ==========================================
# 6. SNAPSHOT DISPLAY + FILE WRITER
# ==========================================

def _fmt(val, decimals: int = 2) -> str:
    """
    Safe float formatter. Never raises ValueError.
    Returns 'N/A' for None, otherwise formats with given decimal places.
    """
    if val is None:
        return "N/A"
    try:
        return f"{float(val):,.{decimals}f}"
    except (ValueError, TypeError):
        return str(val)


def build_snapshot_display(ctx: dict) -> str:
    """Assembles the full console/file snapshot display from the context dict."""

    now        = ctx.get('timestamp', datetime.datetime.now(IST))
    spot       = ctx.get('spot', 0)
    expiry_str = ctx.get('expiry_str', 'N/A')
    vwap       = ctx.get('vwap')
    regime     = ctx.get('regime', 'N/A')
    momentum   = ctx.get('momentum', 'N/A')
    htf_bias   = ctx.get('htf_bias', 'N/A')
    support    = ctx.get('support', 0)
    resistance = ctx.get('resistance', 0)
    trap       = ctx.get('trap_status', 'None')
    entry_dec  = ctx.get('entry_decision', 'NO_TRADE')
    trade_pass = ctx.get('trade_quality_pass', False)
    active     = ctx.get('active_trade')

    regime_data = ctx.get('regime_data', {})
    p_price     = regime_data.get('price', {})
    p_oi        = regime_data.get('oi', {})
    p_vol       = regime_data.get('volume', {})

    # Computed display values
    market_open = datetime.time(9, 15)
    mins_open   = max(
        (now.hour * 60 + now.minute) - (market_open.hour * 60 + market_open.minute), 0
    )

    try:
        exp_dt = datetime.datetime.strptime(expiry_str, "%d-%b-%Y").date()
        dte    = (exp_dt - now.date()).days
    except ValueError:
        dte = "N/A"

    wtd_call_oi  = ctx.get('weighted_call_oi', 0)
    wtd_put_oi   = ctx.get('weighted_put_oi', 0)
    delta_call   = ctx.get('delta_call_total', 0)
    delta_put    = ctx.get('delta_put_total', 0)
    call_vel     = ctx.get('call_velocity', 0)
    put_vel      = ctx.get('put_velocity', 0)
    call_accel   = ctx.get('call_acceleration', 0)
    put_accel    = ctx.get('put_acceleration', 0)
    z_dc         = ctx.get('z_delta_call', 0.0)
    z_dp         = ctx.get('z_delta_put', 0.0)
    z_pcr        = ctx.get('z_pcr_trend', 0.0)
    z_grad       = ctx.get('z_gradient', 0.0)
    base_score   = ctx.get('base_score', 0.0)
    bias_label   = ctx.get('bias_label', 'Neutral')
    confidence   = ctx.get('confidence', 0.0)
    breakout_p   = ctx.get('breakout_probability', 'Low')
    pcr          = ctx.get('pcr', 0.0)
    pcr_trend    = ctx.get('pcr_trend', 0.0)
    persist_put  = ctx.get('persistent_put_writer', False)
    persist_call = ctx.get('persistent_call_writer', False)

    regime_display = {
        "TREND_UP":        "Strong Uptrend",
        "TREND_DOWN":      "Strong Downtrend",
        "WEAK_TREND_UP":   "Weak Uptrend",
        "WEAK_TREND_DOWN": "Weak Downtrend",
        "RANGE":           "Range",
        "VOLATILE":        "Volatile",
    }.get(regime, regime)

    sep  = "=" * 60
    dash = "-" * 60

    lines = [
        sep,
        "📈 NIFTY SIGNAL ENGINE SNAPSHOT",
        sep,
        f"{'Timestamp':<22}: {now.strftime('%Y-%m-%d %H:%M:%S')}",
        f"{'Spot Price':<22}: {_fmt(spot)}",
        f"{'Time Since Open (min)':<22}: {mins_open}",
        f"{'Days to Expiry (DTE)':<22}: {dte}",
        dash,
        "📊 RAW QUANTITATIVE METRICS (ATM ±3 Strikes)",
        dash,
        f"{'Weighted Call OI':<22}: {int(wtd_call_oi):,}",
        f"{'Weighted Put OI':<22}: {int(wtd_put_oi):,}",
        f"{'Delta Call OI':<22}: {int(delta_call):,}",
        f"{'Delta Put OI':<22}: {int(delta_put):,}",
        f"{'Call Velocity (15m)':<22}: {int(call_vel):,}",
        f"{'Put Velocity (15m)':<22}: {int(put_vel):,}",
        f"{'Call Acceleration':<22}: {int(call_accel):,}",
        f"{'Put Acceleration':<22}: {int(put_accel):,}",
        dash,
        "⚖️  Z-SCORE NORMALIZATION (Standard Deviations)",
        dash,
        f"{'Delta Call Norm (Z)':<22}: {z_dc:+.2f}",
        f"{'Delta Put Norm (Z)':<22}: {z_dp:+.2f}",
        f"{'PCR Trend Norm (Z)':<22}: {z_pcr:+.2f}",
        f"{'Gradient Norm (Z)':<22}: {z_grad:+.2f}",
        dash,
        "🎯 SIGNAL ENGINE OUTPUT",
        dash,
        f"{'Base Score':<22}: {base_score:+.2f}",
        f"{'Bias':<22}: {bias_label}",
        f"{'Confidence (0~3+)':<22}: {confidence:.2f}",
        f"{'Market Regime':<22}: {regime_display}",
        f"{'Breakout Probability':<22}: {breakout_p}",
        f"{'Trap Status':<22}: {trap}",
        dash,
        "📌 KEY LEVELS & TRENDS",
        dash,
        f"{'Support Level':<22}: {_fmt(support, 1)}",
        f"{'Resistance Level':<22}: {_fmt(resistance, 1)}",
        f"{'VWAP':<22}: {_fmt(vwap)}",
        f"{'Smart PCR':<22}: {_fmt(pcr)}  (Trend: {pcr_trend:+.2f})",
        f"{'Persistent Put Writer':<22}: {'YES' if persist_put else 'NO'}",
        f"{'Persistent Call Writer':<22}: {'YES' if persist_call else 'NO'}",
        sep,
        "",
        sep,
        "🔬 REGIME PILLARS DETAIL",
        sep,
        f"{'Price Direction':<22}: {_fmt(p_price.get('direction_pts'), 1)} pts",
        f"{'Price Range':<22}: {_fmt(p_price.get('range_pts'), 1)} pts",
        f"{'Price Efficiency':<22}: {_fmt(p_price.get('efficiency'), 3)}",
        f"{'OI Bias':<22}: {p_oi.get('bias', 'N/A')}",
        f"{'Net Call OI Change':<22}: {int(p_oi.get('net_call_change', 0)):,}",
        f"{'Net Put OI Change':<22}: {int(p_oi.get('net_put_change', 0)):,}",
        f"{'Both OI Surging':<22}: {'YES' if p_oi.get('both_surging') else 'NO'}",
        f"{'Participation':<22}: {p_vol.get('participation', 'N/A')}",
        f"{'Vol Ratio (curr/avg)':<22}: {_fmt(p_vol.get('vol_ratio'), 3)}",
        sep,
        "",
        sep,
        "💼 ACTIVE TRADE STATUS",
        sep,
    ]

    if active:
        open_str = active['entry_time'].strftime('%H:%M:%S') if active.get('entry_time') else "N/A"
        lines += [
            f"{'Status':<22}: OPEN",
            f"{'Type':<22}: {active.get('type', 'N/A')}",
            f"{'Strike':<22}: {active.get('option_strike', 'N/A')}",
            f"{'Entry Option LTP':<22}: Rs {_fmt(active.get('option_entry_ltp'))}",
            f"{'Entry Spot':<22}: {_fmt(active.get('entry_spot'))}",
            f"{'Lots':<22}: {active.get('lots', 'N/A')}",
            f"{'Stop Loss (spot)':<22}: {_fmt(active.get('stop_loss'))}",
            f"{'Open Since':<22}: {open_str}",
        ]
    else:
        lines.append(f"{'Status':<22}: No active trade")

    lines += ["", sep]
    lines += _build_trade_recommendation(ctx)
    lines += [sep, ""]

    return "\n".join(lines)


def _build_trade_recommendation(ctx: dict) -> list:
    """Builds the actionable recommendations block. Returns list of strings."""

    spot        = ctx.get('spot', 0)
    support     = ctx.get('support', 0)
    resistance  = ctx.get('resistance', 0)
    regime      = ctx.get('regime', 'VOLATILE')
    momentum    = ctx.get('momentum', 'WEAK')
    htf_bias    = ctx.get('htf_bias', 'RANGE')
    vwap        = ctx.get('vwap')
    entry_dec   = ctx.get('entry_decision', 'NO_TRADE')
    trade_pass  = ctx.get('trade_quality_pass', False)
    trap_status = ctx.get('trap_status', 'None')
    confidence  = ctx.get('confidence', 0.0)
    bullish_sc  = ctx.get('bullish_score', 0)
    bearish_sc  = ctx.get('bearish_score', 0)
    active      = ctx.get('active_trade')
    daily_pnl   = ctx.get('daily_pnl', 0.0)
    trades_done = ctx.get('trades_today', 0)

    # Probability score
    prob_score    = 0
    score_factors = []

    if regime in ("TREND_UP", "TREND_DOWN"):
        prob_score += 30
        score_factors.append("Confirmed trend (+30)")
    elif regime in ("WEAK_TREND_UP", "WEAK_TREND_DOWN"):
        prob_score += 15
        score_factors.append("Weak trend (+15)")

    if momentum in ("STRONG_UP", "STRONG_DOWN"):
        prob_score += 20
        score_factors.append("Strong momentum (+20)")

    if trap_status == "None":
        prob_score += 15
        score_factors.append("No trap (+15)")

    if htf_bias in ("BULLISH", "BEARISH"):
        prob_score += 15
        score_factors.append(f"HTF {htf_bias} (+15)")

    if vwap is not None:
        vwap_aligned = (
            (entry_dec == "CALL" and spot > vwap) or
            (entry_dec == "PUT"  and spot < vwap)
        )
        if vwap_aligned:
            prob_score += 10
            score_factors.append("VWAP aligned (+10)")

    if bullish_sc >= 2 or bearish_sc >= 2:
        prob_score += 10
        score_factors.append("Strong OI score (+10)")

    prob_score = min(prob_score, 100)

    # Reversal chance
    if trap_status != "None":
        reversal = "HIGH — trap detected"
    elif regime == "VOLATILE":
        reversal = "MEDIUM — volatile regime"
    elif regime in ("WEAK_TREND_UP", "WEAK_TREND_DOWN"):
        reversal = "MEDIUM — unconfirmed trend"
    elif momentum == "WEAK":
        reversal = "MEDIUM — weak momentum"
    else:
        reversal = "LOW — trend confirmed with OI"

    # Trade plan
    if entry_dec == "CALL" and trade_pass:
        atm_s    = round(spot / 50) * 50
        t_strike = atm_s - 50
        sl_spot  = support - 15
        tgt_spot = spot + (spot - support)
        plan = [
            f"  Direction       : BUY CALL",
            f"  Strike          : {t_strike} CE (slightly ITM)",
            f"  Entry Spot Zone : {_fmt(spot)} - {_fmt(spot + 10)}",
            f"  Stop Loss (spot): {_fmt(sl_spot)}  (below support {_fmt(support, 1)})",
            f"  Target (spot)   : {_fmt(tgt_spot)}  (1:1 risk-reward)",
            f"  Position Size   : {calculate_position_size(max(t_strike * 0.005, 1))} lot(s)",
            f"  Max Loss (Rs)   : ~Rs {CAPITAL * RISK_PER_TRADE_PCT:,.0f}",
        ]
    elif entry_dec == "PUT" and trade_pass:
        atm_s    = round(spot / 50) * 50
        t_strike = atm_s + 50
        sl_spot  = resistance + 15
        tgt_spot = spot - (resistance - spot)
        plan = [
            f"  Direction       : BUY PUT",
            f"  Strike          : {t_strike} PE (slightly ITM)",
            f"  Entry Spot Zone : {_fmt(spot - 10)} - {_fmt(spot)}",
            f"  Stop Loss (spot): {_fmt(sl_spot)}  (above resistance {_fmt(resistance, 1)})",
            f"  Target (spot)   : {_fmt(tgt_spot)}  (1:1 risk-reward)",
            f"  Position Size   : {calculate_position_size(max(t_strike * 0.005, 1))} lot(s)",
            f"  Max Loss (Rs)   : ~Rs {CAPITAL * RISK_PER_TRADE_PCT:,.0f}",
        ]
    elif active:
        plan = [
            f"  Trade is OPEN — monitor for exit signals",
            f"  Type   : {active.get('type')} {active.get('option_strike')}",
            f"  SL     : {_fmt(active.get('stop_loss'))} (spot)",
        ]
    else:
        plan = [
            f"  No trade this cycle.",
            f"  Regime: {regime}  |  Trap: {trap_status}",
            f"  Wait for confirmed trend + OI alignment.",
        ]

    sep  = "=" * 60
    dash = "-" * 60

    lines = [
        sep,
        "📋 TRADE-READY RECOMMENDATIONS",
        sep,
        "",
        "── MARKET TREND & STRENGTH",
        f"  Regime          : {regime}",
        f"  Momentum        : {momentum}",
        f"  HTF Bias        : {htf_bias}",
        f"  VWAP            : {_fmt(vwap)}",
        f"  Market Strength : {'STRONG' if regime in ('TREND_UP','TREND_DOWN') else 'WEAK/NEUTRAL'}",
        "",
        "── PROBABILITY SCORING",
        f"  Win Probability : {prob_score}%",
        f"  Confidence Score: {confidence:.2f}",
        "  Factors:",
    ]
    for f in score_factors:
        lines.append(f"    + {f}")
    if not score_factors:
        lines.append("    (no positive factors this cycle)")

    lines += [
        "",
        "── REVERSAL RISK",
        f"  Chance of Reversal: {reversal}",
        f"  Trap Status       : {trap_status}",
        "",
        "── ENTRY / EXIT RULES",
        f"  Signal            : {entry_dec}",
        f"  Quality Filter    : {'PASS' if trade_pass else 'FAIL — do not enter'}",
        "",
        "── TRADE PLAN",
    ]
    lines += plan
    lines += [
        "",
        "── RISK MANAGEMENT",
        f"  Capital           : Rs {CAPITAL:,}",
        f"  Risk / Trade      : {RISK_PER_TRADE_PCT*100:.1f}%  (Rs {CAPITAL*RISK_PER_TRADE_PCT:,.0f})",
        f"  Daily Loss Limit  : Rs {abs(MAX_DAILY_LOSS):,.0f}",
        f"  Trades Today      : {trades_done} / 3",
        f"  Daily P&L         : Rs {daily_pnl:,.2f}",
        f"  Premium Hard SL   : {MAX_PREMIUM_LOSS_PCT*100:.0f}% of entry premium",
        f"  Trail Stop Trigger: {TRAIL_LOCK_TRIGGER*100:.0f}% gain locks "
        f"{TRAIL_LOCK_PCT*100:.0f}% of peak gain",
        "",
    ]

    return lines


def write_latest_calculations(display_text: str) -> None:
    """Overwrites latest-calculations.txt every run."""
    try:
        with open(LATEST_CALC_FILE, 'w', encoding='utf-8') as f:
            f.write(display_text)
        logger.info(f"Written: {LATEST_CALC_FILE}")
    except Exception as e:
        logger.error(f"Failed writing {LATEST_CALC_FILE}: {e}")


# ==========================================
# 7. MASTER PIPELINE  (returns context dict)
# ==========================================

def run_master_pipeline(current_timestamp: datetime.datetime, expiry_date_str: str) -> dict:
    """
    Full analysis + trade execution cycle.
    Returns a rich context dict used by build_snapshot_display().
    """
    global ACTIVE_TRADE, TRADES_TODAY, LAST_TRADE_TIME
    global CURRENT_TRADING_DAY, DAILY_PNL

    if CURRENT_TRADING_DAY != current_timestamp.date():
        TRADES_TODAY        = 0
        DAILY_PNL           = 0.0
        CURRENT_TRADING_DAY = current_timestamp.date()

    ctx = {
        "timestamp":    current_timestamp,
        "expiry_str":   expiry_date_str,
        "daily_pnl":    DAILY_PNL,
        "trades_today": TRADES_TODAY,
        "active_trade": ACTIVE_TRADE,
    }

    # Populate zero-value defaults so display never KeyErrors
    _zero_ctx(ctx)

    if DAILY_PNL <= MAX_DAILY_LOSS:
        logger.warning(f"Daily loss limit hit (Rs{DAILY_PNL:.2f}).")
        ctx['entry_decision']      = "NO_TRADE (Daily limit)"
        ctx['trade_quality_pass']  = False
        return ctx

    with sqlite3.connect(DB_NAME) as conn:
        df_all = pd.read_sql("SELECT * FROM raw_snapshots ORDER BY timestamp DESC", conn)
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp']).dt.tz_localize(IST)
        if df_all.empty:
            ctx['entry_decision'] = "NO_TRADE (No data)"
            return ctx

        df_today = df_all[df_all['timestamp'].dt.date == current_timestamp.date()]

        vwap     = calculate_session_vwap(df_today)
        recent_3 = get_recent_candle_spots(df_all, n_candles=3)
        momentum = calculate_momentum(recent_3)

        df_t0 = df_all[df_all['timestamp'] == current_timestamp]
        if df_t0.empty:
            ctx['entry_decision'] = "NO_TRADE (Snapshot missing)"
            return ctx

        spot_t0  = float(df_t0['spot_price'].iloc[0])
        htf_bias = get_higher_tf_bias(spot_t0)

        target_time    = current_timestamp - datetime.timedelta(minutes=FETCH_INTERVAL_MINUTES)
        past_snapshots = df_all[df_all['timestamp'] <= target_time]
        if past_snapshots.empty:
            ctx.update({"spot": spot_t0, "vwap": vwap, "momentum": momentum,
                        "htf_bias": htf_bias, "entry_decision": "NO_TRADE (Warming up)"})
            return ctx

        t1_timestamp = past_snapshots['timestamp'].max()
        df_t1        = past_snapshots[past_snapshots['timestamp'] == t1_timestamp]
        if df_t1.empty:
            ctx['entry_decision'] = "NO_TRADE"
            return ctx

        spot_t1 = float(df_t1['spot_price'].iloc[0])

        # ── Support / Resistance ─────────────────────────────────────────
        atm_strike = round(spot_t0 / 50) * 50
        df_near    = df_t0[
            (df_t0['strike'] >= atm_strike - STRIKE_WINDOW) &
            (df_t0['strike'] <= atm_strike + STRIKE_WINDOW)
        ].copy()
        df_near_t1 = df_t1[
            (df_t1['strike'] >= atm_strike - STRIKE_WINDOW) &
            (df_t1['strike'] <= atm_strike + STRIKE_WINDOW)
        ].copy()

        if df_near.empty or df_near_t1.empty:
            ctx['entry_decision'] = "NO_TRADE"
            return ctx

        prev_call_map = df_near_t1.set_index('strike')['call_oi']
        prev_put_map  = df_near_t1.set_index('strike')['put_oi']
        df_near['delta_call'] = (
            df_near['call_oi'] -
            df_near['strike'].map(prev_call_map).fillna(df_near['call_oi'])
        )
        df_near['delta_put'] = (
            df_near['put_oi'] -
            df_near['strike'].map(prev_put_map).fillna(df_near['put_oi'])
        )

        support    = float(df_near.loc[df_near['put_oi'].idxmax(), 'strike'])
        resistance = float(df_near.loc[df_near['call_oi'].idxmax(), 'strike'])

        # ── 3-Pillar Regime ──────────────────────────────────────────────
        recent_10   = get_recent_candle_spots(df_all, n_candles=10)
        regime_data = detect_market_regime(recent_10, df_near, df_all)
        regime      = regime_data["label"]

        # ── Behavioural Scores ───────────────────────────────────────────
        price_up    = spot_t0 > spot_t1
        price_down  = spot_t0 < spot_t1
        bullish_score = 0
        bearish_score = 0
        trap_status   = "None"

        if price_up:
            if df_near['delta_call'].sum() < -MIN_OI_CHANGE_THRESH:
                bullish_score += 1
            if df_near['delta_put'].sum() > MIN_OI_CHANGE_THRESH:
                bullish_score += 1
            if df_near['delta_call'].sum() > MIN_OI_CHANGE_THRESH:
                trap_status = "BULL TRAP! (Price up, but Calls written or Puts dropped)"

        if price_down:
            if df_near['delta_put'].sum() < -MIN_OI_CHANGE_THRESH:
                bearish_score += 1
            if df_near['delta_call'].sum() > MIN_OI_CHANGE_THRESH:
                bearish_score += 1
            if df_near['delta_put'].sum() > MIN_OI_CHANGE_THRESH:
                trap_status = "BEAR TRAP! (Price down, but Puts written or Calls dropped)"

        # ── OI Display Metrics ───────────────────────────────────────────
        atm_range = df_near[
            (df_near['strike'] >= atm_strike - 150) &
            (df_near['strike'] <= atm_strike + 150)
        ]
        if not atm_range.empty:
            weights     = np.exp(-0.5 * ((atm_range['strike'].values - atm_strike) / 100) ** 2)
            wtd_call_oi = float(np.sum(atm_range['call_oi'].values * weights))
            wtd_put_oi  = float(np.sum(atm_range['put_oi'].values  * weights))
        else:
            wtd_call_oi = 0.0
            wtd_put_oi  = 0.0

        delta_call_total = float(df_near['delta_call'].sum())
        delta_put_total  = float(df_near['delta_put'].sum())
        call_velocity    = delta_call_total
        put_velocity     = delta_put_total

        # Acceleration via 3-snapshot second derivative
        ts_list = (
            df_all.groupby('timestamp')['spot_price']
            .first().sort_index(ascending=True).tail(5).index.tolist()
        )
        call_accel = put_accel = 0

        def _oi_at(ts):
            df_s = df_all[df_all['timestamp'] == ts]
            df_s = df_s[
                (df_s['strike'] >= atm_strike - STRIKE_WINDOW) &
                (df_s['strike'] <= atm_strike + STRIKE_WINDOW)
            ]
            return float(df_s['call_oi'].sum()), float(df_s['put_oi'].sum())

        if len(ts_list) >= 3:
            c1, p1 = _oi_at(ts_list[-3])
            c2, p2 = _oi_at(ts_list[-2])
            c3, p3 = _oi_at(ts_list[-1])
            call_accel = (c3 - c2) - (c2 - c1)
            put_accel  = (p3 - p2) - (p2 - p1)

        # PCR
        total_call_oi = float(df_near['call_oi'].sum())
        total_put_oi  = float(df_near['put_oi'].sum())
        pcr           = (total_put_oi / total_call_oi) if total_call_oi > 0 else 0.0
        prior_call    = float(df_near_t1['call_oi'].sum())
        prior_put     = float(df_near_t1['put_oi'].sum())
        prior_pcr     = (prior_put / prior_call) if prior_call > 0 else 0.0
        pcr_trend     = pcr - prior_pcr

        # Z-scores (rolling 10-snapshot window)
        hist_c, hist_p = [], []
        for ts in ts_list:
            df_s = df_all[df_all['timestamp'] == ts]
            df_s = df_s[
                (df_s['strike'] >= atm_strike - STRIKE_WINDOW) &
                (df_s['strike'] <= atm_strike + STRIKE_WINDOW)
            ]
            hist_c.append(float(df_s['call_oi'].sum()))
            hist_p.append(float(df_s['put_oi'].sum()))

        def _zscore(series, val):
            if len(series) < 2:
                return 0.0
            mu  = float(np.mean(series))
            std = float(np.std(series))
            return (val - mu) / std if std > 0 else 0.0

        z_dc   = _zscore(hist_c, delta_call_total)
        z_dp   = _zscore(hist_p, delta_put_total)
        z_pcr  = _zscore([1.0] * max(len(ts_list), 2), pcr_trend)
        z_grad = (z_dc + z_dp) / 2

        base_score = float(bullish_score - bearish_score)
        confidence = abs(z_dc) * 0.4 + abs(z_dp) * 0.4 + abs(z_grad) * 0.2

        if bullish_score > bearish_score:
            bias_label = "Bullish"
        elif bearish_score > bullish_score:
            bias_label = "Bearish"
        else:
            bias_label = "Neutral"

        breakout_prob = (
            "High"   if confidence > 2.0 and trap_status == "None" else
            "Medium" if confidence > 1.0 else
            "Low"
        )
        persist_put  = delta_put_total  > MIN_OI_CHANGE_THRESH
        persist_call = delta_call_total > MIN_OI_CHANGE_THRESH

        signal = {
            "timestamp":       current_timestamp,
            "spot":            spot_t0,
            "spot_t1":         spot_t1,
            "support":         support,
            "resistance":      resistance,
            "bullish_score":   bullish_score,
            "bearish_score":   bearish_score,
            "trap_status":     trap_status,
            "vwap":            vwap,
            "regime":          regime,
            "regime_data":     regime_data,
            "momentum":        momentum,
            "htf_bias":        htf_bias,
            "type_considered": None,
        }

        # ── Active Trade Management ──────────────────────────────────────
        if ACTIVE_TRADE is not None:
            current_opt_ltp = get_current_option_ltp(df_t0, ACTIVE_TRADE)

            if current_opt_ltp is None:
                logger.warning("Option data gone — forcing exit.")
                current_opt_ltp = ACTIVE_TRADE['option_entry_ltp']
                exit_decision   = "FULL_EXIT (No option data)"
            else:
                exit_decision = enhanced_exit_logic(
                    ACTIVE_TRADE, signal, current_opt_ltp, expiry_date_str
                )

            if "FULL_EXIT" in exit_decision:
                pnl = calculate_pnl(ACTIVE_TRADE, current_opt_ltp)
                LAST_10_TRADES.append(1 if pnl > 0 else 0)
                DAILY_PNL += pnl

                logger.info(
                    f"TRADE CLOSED: {exit_decision} | "
                    f"Entry Rs{ACTIVE_TRADE['option_entry_ltp']:.2f} -> "
                    f"Exit Rs{current_opt_ltp:.2f} | PnL Rs{pnl:.2f}"
                )

                conn.execute(
                    '''INSERT INTO trade_log
                       (timestamp, action, type, option_strike,
                        entry_spot, exit_spot,
                        entry_option_ltp, exit_option_ltp,
                        pnl, lots, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (
                        current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'EXIT', ACTIVE_TRADE['type'], ACTIVE_TRADE['option_strike'],
                        ACTIVE_TRADE['entry_spot'], spot_t0,
                        ACTIVE_TRADE['option_entry_ltp'], current_opt_ltp,
                        pnl, ACTIVE_TRADE['lots'], exit_decision,
                    )
                )
                conn.commit()
                ACTIVE_TRADE = None

            ctx['entry_decision']     = exit_decision
            ctx['trade_quality_pass'] = False

        else:
            # ── New Entry ────────────────────────────────────────────────
            entry_dec  = "NO_TRADE"
            trade_pass = False

            if TRADES_TODAY < 3:
                cooldown_ok = True
                if LAST_TRADE_TIME:
                    elapsed     = (datetime.datetime.now(IST) - LAST_TRADE_TIME).total_seconds()
                    cooldown_ok = elapsed >= 600

                if cooldown_ok:
                    entry_dec = evaluate_entry(signal)

                    if entry_dec != "NO_TRADE":
                        signal['type_considered'] = entry_dec
                        trade_pass = final_trade_filter(signal)

                        if trade_pass:
                            is_valid, strike, opt_ltp = validate_execution_and_liquidity(
                                df_t0, spot_t0, spot_t1, entry_dec
                            )
                            if is_valid:
                                perf_mult = performance_guard()
                                base_lots = calculate_position_size(opt_ltp)
                                lots      = max(1, int(base_lots * perf_mult))
                                sl        = (support - 15 if entry_dec == "CALL"
                                             else resistance + 15)

                                ACTIVE_TRADE = {
                                    "type":               entry_dec,
                                    "entry_time":         current_timestamp,
                                    "entry_spot":         spot_t0,
                                    "option_strike":      strike,
                                    "option_entry_ltp":   opt_ltp,
                                    "peak_option_ltp":    opt_ltp,
                                    "premium_trail_stop": None,
                                    "stop_loss":          sl,
                                    "lots":               lots,
                                }
                                TRADES_TODAY   += 1
                                LAST_TRADE_TIME = datetime.datetime.now(IST)

                                conn.execute(
                                    '''INSERT INTO trade_log
                                       (timestamp, action, type, option_strike,
                                        entry_spot, entry_option_ltp, lots, notes)
                                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                    (
                                        current_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                                        'ENTRY', entry_dec, strike, spot_t0, opt_ltp,
                                        lots,
                                        (f"Regime:{regime} HTF:{htf_bias} "
                                         f"Mom:{momentum} "
                                         f"VWAP:{_fmt(vwap) if vwap is not None else 'N/A'}"),
                                    )
                                )
                                conn.commit()
                                logger.info(
                                    f"EXECUTED {entry_dec} | Strike {strike} "
                                    f"@ Rs{opt_ltp:.2f} | Lots {lots}"
                                )
                            else:
                                trade_pass = False

            ctx['entry_decision']     = entry_dec
            ctx['trade_quality_pass'] = trade_pass

    # ── Populate full context dict ───────────────────────────────────────
    ctx.update({
        "spot":                   spot_t0,
        "spot_t1":                spot_t1,
        "vwap":                   vwap,
        "regime":                 regime,
        "regime_data":            regime_data,
        "momentum":               momentum,
        "htf_bias":               htf_bias,
        "support":                support,
        "resistance":             resistance,
        "bullish_score":          bullish_score,
        "bearish_score":          bearish_score,
        "trap_status":            trap_status,
        "weighted_call_oi":       wtd_call_oi,
        "weighted_put_oi":        wtd_put_oi,
        "delta_call_total":       delta_call_total,
        "delta_put_total":        delta_put_total,
        "call_velocity":          call_velocity,
        "put_velocity":           put_velocity,
        "call_acceleration":      call_accel,
        "put_acceleration":       put_accel,
        "pcr":                    pcr,
        "pcr_trend":              pcr_trend,
        "z_delta_call":           z_dc,
        "z_delta_put":            z_dp,
        "z_pcr_trend":            z_pcr,
        "z_gradient":             z_grad,
        "base_score":             base_score,
        "bias_label":             bias_label,
        "confidence":             confidence,
        "breakout_probability":   breakout_prob,
        "persistent_put_writer":  persist_put,
        "persistent_call_writer": persist_call,
        "active_trade":           ACTIVE_TRADE,
        "daily_pnl":              DAILY_PNL,
        "trades_today":           TRADES_TODAY,
    })

    return ctx


def _zero_ctx(ctx: dict) -> None:
    """Pre-fills context dict with safe zero/None defaults."""
    defaults = {
        "spot": 0, "spot_t1": 0, "vwap": None, "regime": "VOLATILE",
        "regime_data": {}, "momentum": "WEAK", "htf_bias": "RANGE",
        "support": 0, "resistance": 0, "bullish_score": 0, "bearish_score": 0,
        "trap_status": "None", "weighted_call_oi": 0, "weighted_put_oi": 0,
        "delta_call_total": 0, "delta_put_total": 0, "call_velocity": 0,
        "put_velocity": 0, "call_acceleration": 0, "put_acceleration": 0,
        "pcr": 0.0, "pcr_trend": 0.0, "z_delta_call": 0.0, "z_delta_put": 0.0,
        "z_pcr_trend": 0.0, "z_gradient": 0.0, "base_score": 0.0,
        "bias_label": "Neutral", "confidence": 0.0, "breakout_probability": "Low",
        "persistent_put_writer": False, "persistent_call_writer": False,
        "entry_decision": "NO_TRADE", "trade_quality_pass": False,
    }
    for k, v in defaults.items():
        ctx.setdefault(k, v)


# ==========================================
# MAIN — Continuous Scheduler Mode
# ==========================================
def main():
    init_db()
    fetch_and_cache_nse_holidays()
    
    logger.info("Nifty Signal Engine — Internal Scheduler Mode Started")
    last_run_minute = None

    while True:
        now = datetime.datetime.now(IST)

        # 1. Market days guard (Weekends & Holidays)
        if now.weekday() >= 5 or now.strftime('%Y-%m-%d') in NSE_HOLIDAYS:
            logger.info(f"Market Closed (Weekend/Holiday). Sleeping for 1 hour...")
            time.sleep(3600)
            continue

        # 2. Market hours guard
        market_open = datetime.time(9, 15)
        market_close = datetime.time(15, 30)

        if not (market_open <= now.time() <= market_close):
            # Sleep for 1 minute if outside market hours
            time.sleep(60)
            continue

        # 3. Trigger strictly on clock boundaries (e.g., minutes 0, 3, 6... 15, 30, 45)
        current_minute = now.minute

        # Check if we hit a 3-minute interval and haven't already run in this minute
        if current_minute % FETCH_INTERVAL_MINUTES == 0 and current_minute != last_run_minute:
            last_run_minute = current_minute
            logger.info(f"\n=== Starting Scheduled Cycle at {now.strftime('%H:%M')} ===")

            # Reload DB state in case of edge cases or manual edits
            load_trade_state_from_db()

            # Core Fetch & Compute Pipeline
            data, expiry = fetch_nifty_data()
            if not data:
                logger.error("NSE fetch failed this cycle.")
            else:
                ts = store_snapshot(data)
                if ts:
                    ctx = run_master_pipeline(ts, expiry)
                    display = build_snapshot_display(ctx)
                    print(display)
                    write_latest_calculations(display)

                    # 4. Trigger AI & Telegram strictly on 15-minute marks
                    if current_minute % 15 == 0:
                        logger.info("🕒 15-Minute mark reached: Triggering AI Analysis and Telegram")
                        notifier = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nifty_notify.py")
                        if os.path.exists(notifier):
                            try:
                                subprocess.run([sys.executable, notifier], timeout=120, check=False)
                            except Exception as e:
                                logger.warning(f"nifty_notify.py call failed: {e}")
                        else:
                            logger.warning("nifty_notify.py not found.")
                    else:
                        logger.info("AI/Telegram skipped (not a 15-minute mark).")

        # Sleep briefly (10 seconds) to prevent maxing out the CPU while waiting for the next minute
        time.sleep(10)


if __name__ == "__main__":
    main()