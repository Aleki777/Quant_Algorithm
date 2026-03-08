#!/usr/bin/env python3 
"""
Patched demo_bot.py
- Decimal-based price/qty quantization to avoid precision errors (-1111)
- Use /fapi/v1/algoOrder to place TP/SL as CONDITIONAL algos (TAKE_PROFIT_MARKET / STOP_MARKET)
- Use /fapi/v2/positionRisk to reconcile authoritative filled quantity when available
- Per-signal algo watcher that monitors algo status indefinitely (while bot runs) and performs OCO cancellation once one side triggers
- Improvements to formatting and dedupe to avoid stuck retry loops after OCO

Notes:
- This is a single-file integrated replacement derived from your prior version.
- Verify environment variables (BINANCE_API_KEY/SECRET, BINANCE_TESTNET, SYMBOL, etc.) before running.

"""

import os
import time
import json
import math
import hmac
import hashlib
import threading
import traceback
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from decimal import Decimal, getcontext, ROUND_DOWN

# set decimal context precision high enough
getcontext().prec = 18

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import requests
import pandas as pd
import numpy as np
import asyncio
import websockets
from flask import Flask, jsonify

# --------------------
# Config (env overrides)
# --------------------
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_TESTNET = os.getenv("BINANCE_TESTNET", "1").lower() in ("1", "true", "yes")

SYMBOL = os.getenv("SYMBOL", "BTC/USDT")
STREAM_SYMBOL = os.getenv("STREAM_SYMBOL", SYMBOL.replace('/','').lower())
TIMEFRAME = os.getenv("TIMEFRAME", "1h")

def _parse_tf_hours(env_val: str) -> float:
    v = str(env_val).strip()
    if not v:
        return 1.0
    try:
        if v.lower().endswith('m'):
            mins = float(v[:-1])
            return mins / 60.0
        if v.lower().endswith('h'):
            return float(v[:-1])
        return float(v)
    except Exception:
        print(f"[WARN] TF_HOURS invalid value '{env_val}', defaulting to 1 hour")
        return 1.0

TF_HOURS = _parse_tf_hours(os.getenv("TF_HOURS", "1h"))
TARGET_CANDLES = int(os.getenv("TARGET_CANDLES", "1500"))

LEVERAGE = int(os.getenv("LEVERAGE", "10"))

INITIAL_EQUITY = float(os.getenv("INITIAL_EQUITY", "1000.0"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.02"))
MIN_QTY = float(os.getenv("MIN_QTY", "0.0001"))
MAX_USD_PER_TRADE = float(os.getenv("MAX_USD_PER_TRADE", "5000.0"))

AGGRESSIVE_LIMIT_OFFSET = float(os.getenv("AGGRESSIVE_LIMIT_OFFSET", "0.0003"))
LIMIT_TIMEOUT_S = float(os.getenv("LIMIT_TIMEOUT_S", "2"))
TP_MAX_POINTS = float(os.getenv("TP_MAX_POINTS", "150.0"))
TP_R = float(os.getenv("TP_R", "1.5"))
DELTA_REL_MULTIPLIER = float(os.getenv("DELTA_REL_MULTIPLIER", "0.35"))
CONFIRM_MULT = float(os.getenv("CONFIRM_MULT", "0.65"))

MONITOR_POLL_SEC = int(os.getenv("MONITOR_POLL_SEC", "6"))
WATCHDOG_POLL_SEC = int(os.getenv("WATCHDOG_POLL_SEC", "60"))
LISTENKEY_KEEPALIVE_SEC = int(os.getenv("LISTENKEY_KEEPALIVE_SEC", "1500"))

VERBOSE = os.getenv("VERBOSE_LOGGING", "1").lower() in ("1", "true", "yes")

DEMO_FAPI_BASE = "https://demo-fapi.binance.com"

base_rest = DEMO_FAPI_BASE if BINANCE_TESTNET else "https://fapi.binance.com"

exchange_info = None
symbol_info = None

# Dataframe and global state

df = pd.DataFrame()
agg_lock = threading.Lock()
agg_totals: Dict[int, Dict[str, float]] = {}
agg_trades: Dict[int, List[Dict[str, Any]]] = {}
signals_cache: List[Dict[str, Any]] = []
EQUITY = INITIAL_EQUITY

scheduled_signals_set = set()
scheduled_signals_set_lock = threading.Lock()

order_fill_events: Dict[str, Dict[str, Any]] = {}
order_fill_lock = threading.Lock()

order_to_signal: Dict[str, str] = {}
order_to_signal_lock = threading.Lock()

# active algos per signal: signal_id -> {'tp': {...}, 'sl': {...}, 'finalized': bool}
active_algos: Dict[str, Dict[str, Any]] = {}
active_algos_lock = threading.Lock()

listener_thread = None
finalizer_thread = None
watchdog_thread = None
user_ws_thread = None
listen_key = None
listen_key_lock = threading.Lock()

SYMBOL_NO_SLASH = SYMBOL.replace('/', '').upper()

LOT_SIZE_STEP = None
MIN_QTY_STEP = None
PRICE_TICK = None

app = Flask(__name__)

# --------------------
# Decimal helpers (quantize)
# --------------------

def _dec(v) -> Decimal:
    try:
        return Decimal(str(v))
    except Exception:
        return Decimal(0)

def decimal_quantize_exp(step: float) -> Decimal:
    """Return Decimal quantum for given step (e.g. 0.001 -> Decimal('0.001'))"""
    d = Decimal(str(step))
    return d.normalize()

def format_decimal_fixed(d: Decimal) -> str:
    # Remove exponent and trailing zeros but keep necessary decimals
    s = format(d, 'f')
    return s

def round_qty_floor(qty: float) -> Decimal:
    """Floor quantity to LOT_SIZE_STEP using Decimal and ROUND_DOWN."""
    try:
        if LOT_SIZE_STEP is None:
            return Decimal(str(qty))
        q = Decimal(str(qty))
        step = Decimal(str(LOT_SIZE_STEP))
        # compute floored multiple
        multiple = (q // step) * step
        # Ensure at least MIN_QTY_STEP
        if MIN_QTY_STEP is not None:
            minq = Decimal(str(MIN_QTY_STEP))
            if multiple < minq:
                multiple = Decimal('0')
        return multiple.quantize(step, rounding=ROUND_DOWN)
    except Exception:
        return Decimal(str(qty)).quantize(Decimal('1.'), rounding=ROUND_DOWN)

def round_price_tick(price: float) -> Decimal:
    try:
        if PRICE_TICK is None:
            return Decimal(str(price))
        p = Decimal(str(price))
        tick = Decimal(str(PRICE_TICK))
        # floor to tick
        floored = (p // tick) * tick
        return floored.quantize(tick, rounding=ROUND_DOWN)
    except Exception:
        return Decimal(str(price))

# --------------------
# Time helper: ensure_ts_utc (FIX ADDED)
# --------------------
def ensure_ts_utc(val=None):
    """
    Normalize input to a timezone-aware UTC pandas.Timestamp.

    Accepts:
      - None -> returns current UTC timestamp (pd.Timestamp)
      - pandas Timestamp, datetime -> returns tz-aware UTC pd.Timestamp
      - string parseable by pandas.to_datetime -> converted and localized to UTC if naive

    Never raises; on error returns current UTC timestamp.
    """
    try:
        if val is None:
            return pd.Timestamp.now(tz=timezone.utc)
        t = pd.to_datetime(val)
        if pd.isna(t):
            return pd.Timestamp.now(tz=timezone.utc)
        if getattr(t, 'tzinfo', None) is None or t.tz is None:
            return t.tz_localize(timezone.utc)
        else:
            return t.tz_convert(timezone.utc)
    except Exception:
        return pd.Timestamp.now(tz=timezone.utc)

# --------------------
# Server time helper for signing
# --------------------
_server_time_cache = {"ts": 0, "serverTime": None, "lock": threading.Lock(), "ttl": 2.0}

def get_server_time_ms(force_refresh=False):
    now = time.time()
    with _server_time_cache['lock']:
        if (not force_refresh) and _server_time_cache['serverTime'] is not None and (now - _server_time_cache['ts']) < _server_time_cache['ttl']:
            return int(_server_time_cache['serverTime'])
        try:
            res = requests.get(f"{base_rest}/fapi/v1/time", timeout=5)
            res.raise_for_status()
            js = res.json()
            server_ts = int(js.get('serverTime', int(time.time() * 1000)))
            _server_time_cache['serverTime'] = server_ts
            _server_time_cache['ts'] = now
            return int(server_ts)
        except Exception:
            return int(time.time() * 1000)

# --------------------
# REST signing helpers
# --------------------

def _sign_payload_with_secret(query_string: str) -> str:
    return hmac.new(BINANCE_API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

def _build_qs_from_params(params: Dict[str, Any]):
    return '&'.join([f"{k}={requests.utils.quote(str(v), safe='') }" for k, v in sorted(params.items())])

def _signed_get(path: str, params: Dict[str, Any] = None, base: str = None, timestamp_override: int = None):
    if params is None:
        params = {}
    if base is None:
        base = base_rest
    if timestamp_override is None:
        params['timestamp'] = int(get_server_time_ms())
    else:
        params['timestamp'] = int(timestamp_override)
    qs = _build_qs_from_params(params)
    signature = _sign_payload_with_secret(qs)
    url = f"{base}{path}?{qs}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    r = requests.get(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def _signed_post(path: str, params: Dict[str, Any] = None, base: str = None, timestamp_override: int = None):
    if params is None:
        params = {}
    if base is None:
        base = base_rest
    if timestamp_override is None:
        params['timestamp'] = int(get_server_time_ms())
    else:
        params['timestamp'] = int(timestamp_override)
    qs = _build_qs_from_params(params)
    signature = _sign_payload_with_secret(qs)
    url = f"{base}{path}?{qs}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    r = requests.post(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def _signed_delete(path: str, params: Dict[str, Any] = None, base: str = None, timestamp_override: int = None):
    if params is None:
        params = {}
    if base is None:
        base = base_rest
    if timestamp_override is None:
        params['timestamp'] = int(get_server_time_ms())
    else:
        params['timestamp'] = int(timestamp_override)
    qs = _build_qs_from_params(params)
    signature = _sign_payload_with_secret(qs)
    url = f"{base}{path}?{qs}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    r = requests.delete(url, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

def _public_get(path: str, params: Dict[str, Any] = None, base: str = None):
    if params is None:
        params = {}
    if base is None:
        base = base_rest
    r = requests.get(f"{base}{path}", params=params, timeout=15)
    r.raise_for_status()
    return r.json()

# --------------------
# Exchange info & rounding init
# --------------------

def init_exchange_info():
    global exchange_info, symbol_info, LOT_SIZE_STEP, MIN_QTY_STEP, PRICE_TICK
    try:
        info = _public_get('/fapi/v1/exchangeInfo')
        exchange_info = info
        sym = SYMBOL_NO_SLASH
        for s in info.get('symbols', []):
            if s.get('symbol') == sym:
                symbol_info = s
                break
        if symbol_info is None:
            print('[INIT] exchangeInfo: symbol not found in exchangeInfo')
            return
        for f in symbol_info.get('filters', []):
            t = f.get('filterType')
            if t == 'LOT_SIZE':
                LOT_SIZE_STEP = float(f.get('stepSize'))
                MIN_QTY_STEP = float(f.get('minQty'))
            if t == 'PRICE_FILTER':
                PRICE_TICK = float(f.get('tickSize'))
        if VERBOSE:
            print('[INIT] exchangeInfo loaded for', SYMBOL_NO_SLASH, 'LOT_SIZE_STEP=', LOT_SIZE_STEP, 'PRICE_TICK=', PRICE_TICK)
    except Exception as e:
        print('[INIT] Failed to fetch exchangeInfo:', repr(e))
        exchange_info = None

# --------------------
# Klines warmup
# --------------------

def fetch_klines_demo(symbol: str, interval: str, limit: int = 1000, end_time: int = None):
    params = {'symbol': symbol, 'interval': interval, 'limit': limit}
    if end_time:
        params['endTime'] = int(end_time)
    return _public_get('/fapi/v1/klines', params=params)

def init_warmup_klines():
    global df
    try:
        sym = SYMBOL_NO_SLASH
        batch = fetch_klines_demo(sym, TIMEFRAME, limit=TARGET_CANDLES)
        rows = []
        for r in batch:
            ts = int(r[0]); o = float(r[1]); h = float(r[2]); l = float(r[3]); c = float(r[4]); v = float(r[5])
            rows.append({'ts_ms': ts, 'open': o, 'high': h, 'low': l, 'close': c, 'volume': v, 'timestamp': pd.to_datetime(ts, unit='ms', utc=True)})
        if rows:
            df = pd.DataFrame(rows).set_index('timestamp', drop=False)
            df.sort_values('ts_ms', inplace=True)
            df['buy_vol_base'] = np.nan; df['sell_vol_base'] = np.nan; df['delta_base_raw'] = np.nan
            df['buy_vol_quote'] = np.nan; df['sell_vol_quote'] = np.nan; df['delta_quote_raw'] = np.nan
            df['delta_base_pct'] = np.nan; df['delta_quote_pct'] = np.nan
            df['delta'] = np.nan
            df['prev_close'] = df['close'].shift(1)
            df['tr'] = df[['high','low','prev_close']].apply(lambda r: max(r['high']-r['low'], abs(r['high']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['high'])), abs(r['low']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['low']))), axis=1)
            df['atr'] = df['tr'].rolling(14, min_periods=1).mean().bfill()
        print('Warmup candles loaded:', len(df))
    except Exception as e:
        print('Warmup fetch failed:', repr(e))
        df = pd.DataFrame()

# --------------------
# ListenKey management & user websocket
# --------------------

def create_listen_key():
    global listen_key
    try:
        res = _signed_post('/fapi/v1/listenKey', params={})
        lk = res.get('listenKey') if isinstance(res, dict) else res
        with listen_key_lock:
            listen_key = lk
        print('[LISTENKEY] created:', listen_key)
        return listen_key
    except Exception as e:
        print('[LISTENKEY] creation failed:', repr(e))
        return None

def keepalive_listen_key_loop():
    global listen_key
    while True:
        try:
            time.sleep(LISTENKEY_KEEPALIVE_SEC)
            with listen_key_lock:
                lk = listen_key
            if not lk:
                create_listen_key()
                continue
            try:
                _signed_post('/fapi/v1/listenKey', params={})
                if VERBOSE:
                    print('[LISTENKEY] keepalive OK')
            except Exception as e:
                print('[LISTENKEY] keepalive failed, recreating:', repr(e))
                create_listen_key()
        except Exception:
            print('[LISTENKEY] exception in keepalive loop', traceback.format_exc())
            time.sleep(10)

async def _user_ws_loop(lk):
    ws_url = f"wss://fstream.binancefuture.com/ws/{lk}"
    print('[USER-WS] connecting to', ws_url)
    try:
        async with websockets.connect(ws_url, ping_interval=60) as ws:
            print('[USER-WS] connected.')
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if 'e' in msg and msg.get('e') == 'ORDER_TRADE_UPDATE':
                    _handle_execution_report(msg)
    except Exception as e:
        print('[USER-WS] exception:', repr(e))

def start_user_ws_in_thread():
    def _run():
        while True:
            with listen_key_lock:
                lk = listen_key
            if not lk:
                time.sleep(2)
                continue
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(_user_ws_loop(lk))
            except Exception as e:
                print('[USER-WS] loop crashed:', repr(e))
            time.sleep(2)
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t

# --------------------
# AggTrade websocket listener
# --------------------
async def _agg_listener_loop():
    WS_BASE = 'wss://fstream.binancefuture.com/ws'
    ws_url = f"{WS_BASE}/{STREAM_SYMBOL}@aggTrade"
    print('Websocket connecting to:', ws_url)
    try:
        async with websockets.connect(ws_url, ping_interval=60) as ws:
            print('AggTrade websocket connected.')
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get('e') != 'aggTrade':
                    continue
                trade_ts = int(msg.get('T'))
                price = float(msg.get('p'))
                qty = float(msg.get('q'))
                quote_qty = price * qty
                isBuyerMaker = msg.get('m')
                taker_is_buy = not bool(isBuyerMaker)
                cs = candle_start_for_ts(trade_ts)
                with agg_lock:
                    if cs not in agg_totals:
                        agg_totals[cs] = {'buy_base':0.0, 'sell_base':0.0, 'buy_quote':0.0, 'sell_quote':0.0, 'count':0}
                        agg_trades[cs] = []
                    if taker_is_buy:
                        agg_totals[cs]['buy_base'] += qty
                        agg_totals[cs]['buy_quote'] += quote_qty
                    else:
                        agg_totals[cs]['sell_base'] += qty
                        agg_totals[cs]['sell_quote'] += quote_qty
                    agg_totals[cs]['count'] += 1
                    agg_trades[cs].append({'ts': trade_ts, 'price': price, 'qty': qty, 'quote_qty': quote_qty, 'taker_is_buy': taker_is_buy})
    except Exception as e:
        print('AggTrade websocket exception:', repr(e))

def start_agg_listener_in_thread():
    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_agg_listener_loop())
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t

# --------------------
# Candle helpers
# --------------------

def candle_start_for_ts(ts_ms, tf_hours=TF_HOURS):
    ms_in_h = 3600_000
    bucket = (ts_ms // (tf_hours * ms_in_h)) * (tf_hours * ms_in_h)
    return int(bucket)

# --------------------
# SR detection & scheduling
# --------------------
# (Existing functions copied unchanged)

def is_support(df1, pos, n1, n2):
    if pos - n1 < 0 or pos + n2 >= len(df1): return False
    for i in range(pos - n1 + 1, pos + 1):
        if df1['low'].iloc[i] > df1['low'].iloc[i-1]:
            return False
    for i in range(pos + 1, pos + n2 + 1):
        if df1['low'].iloc[i] < df1['low'].iloc[i-1]:
            return False
    return True

def is_resistance(df1, pos, n1, n2):
    if pos - n1 < 0 or pos + n2 >= len(df1): return False
    for i in range(pos - n1 + 1, pos + 1):
        if df1['high'].iloc[i] < df1['high'].iloc[i-1]:
            return False
    for i in range(pos + 1, pos + n2 + 1):
        if df1['high'].iloc[i] > df1['high'].iloc[i-1]:
            return False
    return True

def compute_sr_zones_safe(df_local, n1=3, n2=2, dedupe_pct=0.003, zone_buffer_pct=0.002):
    sr_raw = []
    for pos in range(n1, len(df_local)-n2):
        if is_support(df_local, pos, n1, n2):
            sr_raw.append((pos, float(df_local['low'].iloc[pos]), 1))
        if is_resistance(df_local, pos, n1, n2):
            sr_raw.append((pos, float(df_local['high'].iloc[pos]), 2))
    supports = [x[1] for x in sr_raw if x[2]==1]
    resistances = [x[1] for x in sr_raw if x[2]==2]
    def dedupe_by_pct(levels, pct_tol=0.005):
        if not levels: return []
        levels_sorted = sorted(levels)
        cleaned = [levels_sorted[0]]
        for p in levels_sorted[1:]:
            last = cleaned[-1]
            if abs(p - last) / max(1.0, last) > pct_tol:
                cleaned.append(p)
        return cleaned
    clean_supports = dedupe_by_pct(supports, pct_tol=dedupe_pct)
    clean_resistances = dedupe_by_pct(resistances, pct_tol=dedupe_pct)
    atr_med = float(df_local['atr'].median() if 'atr' in df_local.columns else 0.0)
    support_zones = []
    resistance_zones = []
    for lvl in clean_supports:
        pos_candidates = [x[0] for x in sr_raw if x[2]==1 and abs(x[1]-lvl) / max(1.0, lvl) <= dedupe_pct*2]
        pos = int(pos_candidates[0]) if pos_candidates else -1
        buffer = max(lvl * zone_buffer_pct, atr_med * 0.5)
        support_zones.append({"pos": pos, "level": lvl, "low": lvl - buffer, "high": lvl + buffer})
    for lvl in clean_resistances:
        pos_candidates = [x[0] for x in sr_raw if x[2]==2 and abs(x[1]-lvl) / max(1.0, lvl) <= dedupe_pct*2]
        pos = int(pos_candidates[0]) if pos_candidates else -1
        buffer = max(lvl * zone_buffer_pct, atr_med * 0.5)
        resistance_zones.append({"pos": pos, "level": lvl, "low": lvl - buffer, "high": lvl + buffer})
    return {"sr_raw": sr_raw, "supports": clean_supports, "resistances": clean_resistances,
            "support_zones": support_zones, "resistance_zones": resistance_zones}

# detect_signals_on_df_and_schedule unchanged (copied)

def detect_signals_on_df_and_schedule(df_local, support_zones, resistance_zones,
                         sl_buffer_min=50.0,
                         delta_rel_multiplier=DELTA_REL_MULTIPLIER,
                         confirm_mult=CONFIRM_MULT,
                         tp_r=TP_R,
                         stop_padding_atr_mult=0.08,
                         tp_max_points=TP_MAX_POINTS,
                         min_gap=pd.Timedelta('1h')):
    scheduled = []
    if df_local.empty or df_local['delta'].notna().sum() == 0:
        return scheduled
    df_sorted = df_local.sort_values('ts_ms').reset_index(drop=True)
    delta_std = df_sorted['delta'].std(skipna=True)
    delta_threshold = float(delta_std or 0.0) * float(delta_rel_multiplier)
    TP_points = float(tp_max_points)
    SL_points = float(tp_max_points) / float(tp_r) if tp_r != 0 else float(tp_max_points)
    last_entry_time = ensure_ts_utc(pd.Timestamp('1900-01-01'))

    for i in range(len(df_sorted)-2):
        row = df_sorted.iloc[i]
        confirm = df_sorted.iloc[i+1]
        row_ts_ms = int(row['ts_ms'])
        confirm_ts_ms = int(confirm['ts_ms'])
        now_ts = ensure_ts_utc(row['timestamp'])
        if now_ts - last_entry_time < min_gap:
            continue
        row_delta_val = row.get('delta', np.nan)
        conf_delta_val = confirm.get('delta', np.nan)
        atr_here = df_sorted['atr'].iloc[i] if not pd.isna(df_sorted['atr'].iloc[i]) else df_sorted['atr'].mean()
        stop_padding = max(sl_buffer_min, atr_here * stop_padding_atr_mult)

        # LONG
        long_candidate = (not pd.isna(row_delta_val)) and (row_delta_val >= delta_threshold)
        if long_candidate:
            touched_supports = [z for z in support_zones if not (row['high'] < z['low'] or row['low'] > z['high'])]
            if touched_supports:
                zone = touched_supports[0]
                conf_req = (not pd.isna(conf_delta_val)) and (conf_delta_val >= max(0.0, delta_threshold * confirm_mult))
                if (confirm['close'] >= confirm['open']) and conf_req:
                    raw_limit = float(confirm['close'])
                    limit_price = raw_limit * (1.0 + AGGRESSIVE_LIMIT_OFFSET)
                    entry_idx = i + 2
                    if entry_idx >= len(df_sorted):
                        entry_cs = int(confirm_ts_ms + TF_HOURS * 3600 * 1000)
                    else:
                        entry_cs = int(df_sorted['ts_ms'].iloc[entry_idx])
                    scheduled.append({
                        'side':'long','signal_idx':i,'zone_pos':zone.get('pos',-1),'limit_price':limit_price,
                        'planned_limit_raw_close': raw_limit,'entry_cs': entry_cs,'stop_points':SL_points,'tp_points':TP_points
                    })
                    last_entry_time = ensure_ts_utc(confirm['timestamp'])
                    continue

        # SHORT
        short_candidate = (not pd.isna(row_delta_val)) and (row_delta_val <= -delta_threshold)
        if short_candidate:
            touched_res = [z for z in resistance_zones if not (row['high'] < z['low'] or row['low'] > z['high'])]
            if touched_res:
                zone = touched_res[0]
                conf_req = (not pd.isna(conf_delta_val)) and (conf_delta_val <= -max(0.0, delta_threshold * confirm_mult))
                if (confirm['close'] <= confirm['open']) and conf_req:
                    raw_limit = float(confirm['close'])
                    limit_price = raw_limit * (1.0 - AGGRESSIVE_LIMIT_OFFSET)
                    entry_idx = i + 2
                    if entry_idx >= len(df_sorted):
                        entry_cs = int(confirm_ts_ms + TF_HOURS * 3600 * 1000)
                    else:
                        entry_cs = int(df_sorted['ts_ms'].iloc[entry_idx])
                    scheduled.append({
                        'side':'short','signal_idx':i,'zone_pos':zone.get('pos',-1),'limit_price':limit_price,
                        'planned_limit_raw_close': raw_limit,'entry_cs': entry_cs,'stop_points':SL_points,'tp_points':TP_points
                    })
                    last_entry_time = ensure_ts_utc(confirm['timestamp'])
                    continue

    return scheduled

# --------------------
# Sizing helper
# --------------------

def compute_qty_from_risk_usd_live(entry_price: float, stop_price: float, equity_usd: float, risk_pct: float,
                                   min_qty: float = MIN_QTY, max_usd_per_trade: float = MAX_USD_PER_TRADE):
    try:
        risk_price = abs(float(entry_price) - float(stop_price))
        if risk_price <= 0 or equity_usd <= 0 or risk_pct <= 0:
            return 0.0, 0.0, 0.0, False
        dollar_risk = float(equity_usd) * float(risk_pct)
        qty = dollar_risk / risk_price
        notional = qty * float(entry_price)
        size_capped = False
        if max_usd_per_trade is not None and notional > float(max_usd_per_trade):
            qty = float(max_usd_per_trade) / float(entry_price)
            size_capped = True
            notional = qty * float(entry_price)
        if qty < float(min_qty):
            qty = float(min_qty)
            notional = qty * float(entry_price)
        qty = float(qty)
        return float(qty), float(dollar_risk), float(notional), bool(size_capped)
    except Exception:
        return 0.0, 0.0, 0.0, False

# --------------------
# Orders (entry orders) - MARKET and LIMIT
# --------------------

def place_market_order_rest(side: str, qty: float):
    try:
        qty_dec = round_qty_floor(qty)
        if qty_dec <= 0:
            raise ValueError("qty <= 0")
        params = {'symbol': SYMBOL_NO_SLASH, 'side': side.upper(), 'type': 'MARKET', 'quantity': format_decimal_fixed(qty_dec)}
        res = _signed_post('/fapi/v1/order', params=params)
        return res
    except Exception as e:
        print('[ORDER] market order failed:', repr(e))
        return None

def place_limit_ioc_order_rest(side: str, qty: float, price: float):
    try:
        qty_dec = round_qty_floor(qty)
        if qty_dec <= 0:
            raise ValueError("qty <= 0")
        price_dec = round_price_tick(price)
        params = {'symbol': SYMBOL_NO_SLASH, 'side': side.upper(), 'type': 'LIMIT', 'timeInForce': 'IOC',
                  'quantity': format_decimal_fixed(qty_dec), 'price': format_decimal_fixed(price_dec)}
        res = _signed_post('/fapi/v1/order', params=params)
        return res
    except Exception as e:
        print('[ORDER] limit IOC failed:', repr(e))
        return None

def set_leverage_for_symbol(symbol: str, leverage: int):
    try:
        server_ts = get_server_time_ms(force_refresh=True)
        params = {'symbol': symbol, 'leverage': int(leverage)}
        res = _signed_post('/fapi/v1/leverage', params=params, timestamp_override=server_ts)
        print('[INIT] leverage set response:', res)
        return res
    except Exception as e:
        print('[INIT] set leverage failed:', repr(e))
        return None

# --------------------
# Execution report handler
# --------------------

def _handle_execution_report(msg: Dict[str, Any]):
    try:
        data = msg.get('o') if 'o' in msg else msg
        order_id = str(data.get('i') or data.get('orderId') or '')
        status = data.get('X') or data.get('status')
        side = data.get('S') or data.get('side')
        avg_price = None
        executed_qty = 0.0
        try:
            avg_price = float(data.get('ap') or data.get('avgPrice') or 0.0)
        except Exception:
            avg_price = None
        try:
            executed_qty = float(data.get('z') or data.get('executedQty') or 0.0)
        except Exception:
            executed_qty = 0.0

        if VERBOSE:
            print('[EXEC-REPORT] status=', status, 'symbol=', data.get('s') or data.get('symbol'), 'side=', side, 'avg=', avg_price, 'qty=', executed_qty, 'orderId=', order_id)

        if order_id:
            with order_fill_lock:
                prev = order_fill_events.get(order_id, {})
                total_exec = float(prev.get('executedQty', 0.0)) + float(executed_qty)
                use_avg = avg_price if avg_price not in (None, 0.0) else prev.get('avgPrice')
                order_fill_events[order_id] = {
                    'orderId': order_id,
                    'status': status,
                    'executedQty': total_exec,
                    'avgPrice': use_avg,
                    'last_update_ts': int(time.time() * 1000),
                    'raw': data
                }
            with order_to_signal_lock:
                sig = order_to_signal.get(order_id)
            if VERBOSE and sig:
                print(f'[EXEC-REPORT] mapped order {order_id} -> signal {sig} executedQty={total_exec} avgPrice={use_avg}')
    except Exception:
        print('[EXEC-REPORT] parsing error', traceback.format_exc())

# --------------------
# Wait for fill helper (with positionRisk fallback)
# --------------------

def get_position_fill_qty_from_api(symbol: str) -> Optional[Decimal]:
    # Query positionRisk and return absolute positionAmt if present
    try:
        res = _signed_get('/fapi/v2/positionRisk', params={'symbol': symbol})
        # res may be list or dict
        if isinstance(res, list) and res:
            row = res[0]
            pos_amt = Decimal(str(row.get('positionAmt', '0')))
            return abs(pos_amt)
        if isinstance(res, dict):
            pos_amt = Decimal(str(res.get('positionAmt', '0')))
            return abs(pos_amt)
    except Exception as e:
        if VERBOSE:
            print('[DIAG] positionRisk fetch failed:', repr(e))
    return None


def wait_for_fill_for_order(order_id: str, wait_total_s: float = 10.0, poll_interval_s: float = 0.5):
    deadline = time.time() + wait_total_s
    while time.time() < deadline:
        with order_fill_lock:
            evt = order_fill_events.get(str(order_id))
            if evt:
                executed = float(evt.get('executedQty', 0.0))
                status = evt.get('status', '')
                avg = evt.get('avgPrice')
                if executed > 0 or str(status).upper() in ('FILLED','PARTIALLY_FILLED'):
                    return evt
        try:
            r = _signed_get('/fapi/v1/order', params={'symbol': SYMBOL_NO_SLASH, 'orderId': order_id})
            executedQty = float(r.get('executedQty') or 0.0)
            avgPrice = None
            try:
                avgPrice = float(r.get('avgPrice') or 0.0)
            except Exception:
                avgPrice = None
            if executedQty > 0:
                evt = {'orderId': str(order_id), 'executedQty': executedQty, 'avgPrice': avgPrice, 'status': r.get('status', ''), 'raw': r}
                with order_fill_lock:
                    order_fill_events[str(order_id)] = evt
                return evt
        except Exception:
            pass
        time.sleep(poll_interval_s)
    with order_fill_lock:
        return order_fill_events.get(str(order_id))

# --------------------
# Algo endpoints for TP/SL
# --------------------

def place_algo_order(algo_type: str, order_type: str, side: str, trigger_price: Decimal, quantity: Decimal, clientAlgoId: str, max_retries: int = 2, signal_id: str = None):
    # algo_type: 'CONDITIONAL'
    # order_type: 'TAKE_PROFIT_MARKET' or 'STOP_MARKET'
    for attempt in range(1, max_retries + 1):
        try:
            params = {
                'symbol': SYMBOL_NO_SLASH,
                'algoType': algo_type,
                'type': order_type,
                'side': side.upper(),
                'triggerPrice': format_decimal_fixed(trigger_price),
                'workingType': 'CONTRACT_PRICE',
                'clientAlgoId': clientAlgoId,
                'quantity': format_decimal_fixed(quantity)
            }
            res = _signed_post('/fapi/v1/algoOrder', params=params)
            if VERBOSE:
                print(f'[ALGO] placed algo (attempt {attempt}) for signal={signal_id} params={params} resp={res}')
            return res
        except Exception as e:
            err = repr(e)
            print(f'[ALGO] placement attempt {attempt} failed for {order_type}: {err}')
            time.sleep(0.4)
            continue
    return None


def query_algo_order(algoId: Optional[str] = None, clientAlgoId: Optional[str] = None, max_retries: int = 2):
    # GET /fapi/v1/algoOrder?algoId=...&clientAlgoId=...&symbol=...
    for attempt in range(1, max_retries+1):
        try:
            params = {'symbol': SYMBOL_NO_SLASH}
            if algoId:
                params['algoId'] = str(algoId)
            if clientAlgoId:
                params['clientAlgoId'] = str(clientAlgoId)
            res = _signed_get('/fapi/v1/algoOrder', params=params)
            return res
        except Exception as e:
            if VERBOSE:
                print(f'[ALGO-QUERY] query attempt {attempt} failed for algoId={algoId} clientAlgoId={clientAlgoId}:', repr(e))
            time.sleep(0.4)
            continue
    return None


def cancel_algo_order(algoId: Optional[str] = None, clientAlgoId: Optional[str] = None, max_retries: int = 3):
    # DELETE /fapi/v1/algoOrder?algoId=...&clientAlgoId=...&symbol=...
    for attempt in range(1, max_retries+1):
        try:
            params = {'symbol': SYMBOL_NO_SLASH}
            if algoId:
                params['algoId'] = str(algoId)
            if clientAlgoId:
                params['clientAlgoId'] = str(clientAlgoId)
            res = _signed_delete('/fapi/v1/algoOrder', params=params)
            if VERBOSE:
                print(f'[ALGO-CANCEL] cancel attempt {attempt} for algoId={algoId} clientAlgoId={clientAlgoId} resp={res}')
            return res
        except Exception as e:
            if VERBOSE:
                print(f'[ALGO-CANCEL] attempt {attempt} failed for algoId={algoId} clientAlgoId={clientAlgoId}:', repr(e))
            time.sleep(0.5)
            continue
    return None

# --------------------
# Algo watcher (per-signal) - monitors tp & sl algos and performs OCO cancellation/cleanup
# --------------------

def start_algo_watcher(signal_id: str, tp_info: Dict[str, Any], sl_info: Dict[str, Any]):
    """Start a background thread that monitors the two algos for a signal and finalizes OCO."""
    def _thread():
        try:
            key = signal_id
            with active_algos_lock:
                active_algos[key] = {'tp': tp_info.copy(), 'sl': sl_info.copy(), 'finalized': False, 'last_checked': int(time.time()*1000)}

            while True:
                with active_algos_lock:
                    rec = active_algos.get(key)
                    if not rec:
                        break
                    if rec.get('finalized'):
                        break
                # Query both algos status
                tp_algoId = rec['tp'].get('algoId')
                tp_client = rec['tp'].get('clientAlgoId')
                sl_algoId = rec['sl'].get('algoId')
                sl_client = rec['sl'].get('clientAlgoId')
                # Query both; handle missing ids gracefully
                tp_q = query_algo_order(algoId=tp_algoId, clientAlgoId=tp_client)
                sl_q = query_algo_order(algoId=sl_algoId, clientAlgoId=sl_client)

                # Helper to interpret algo query
                def is_triggered(qresp):
                    try:
                        if not qresp:
                            return False, None
                        # qresp could be dict or list; normalize
                        if isinstance(qresp, list):
                            q = qresp[0] if qresp else {}
                        else:
                            q = qresp
                        status = q.get('algoStatus') or q.get('algo_status') or ''
                        if str(status).upper() in ('FILLED','TRIGGERED','TRIGGERED_PARTIAL'):
                            return True, q
                        # also check triggerTime or triggerPrice fields if present
                        return False, q
                    except Exception:
                        return False, None

                tp_triggered, tp_qobj = is_triggered(tp_q)
                sl_triggered, sl_qobj = is_triggered(sl_q)

                if tp_triggered and not rec.get('finalized'):
                    # TP triggered -> cancel SL
                    if VERBOSE:
                        print(f"[OCO] TP triggered for signal={signal_id}, attempting cancel SL algo {sl_algoId} / {sl_client}")
                    cancel_resp = cancel_algo_order(algoId=sl_algoId, clientAlgoId=sl_client)
                    if VERBOSE:
                        print('[OCO] attempted cancel SL', cancel_resp)
                    with active_algos_lock:
                        active_algos[key]['finalized'] = True
                        active_algos[key]['finalized_at'] = int(time.time()*1000)
                        active_algos[key]['finalized_reason'] = 'tp'
                    break

                if sl_triggered and not rec.get('finalized'):
                    if VERBOSE:
                        print(f"[OCO] SL triggered for signal={signal_id}, attempting cancel TP algo {tp_algoId} / {tp_client}")
                    cancel_resp = cancel_algo_order(algoId=tp_algoId, clientAlgoId=tp_client)
                    if VERBOSE:
                        print('[OCO] attempted cancel TP', cancel_resp)
                    with active_algos_lock:
                        active_algos[key]['finalized'] = True
                        active_algos[key]['finalized_at'] = int(time.time()*1000)
                        active_algos[key]['finalized_reason'] = 'sl'
                    break

                # If neither triggered, sleep a bit and continue
                time.sleep(max(2.0, MONITOR_POLL_SEC))
                with active_algos_lock:
                    active_algos[key]['last_checked'] = int(time.time()*1000)

        except Exception:
            print('[ALGO-WATCHER] exception', traceback.format_exc())
        finally:
            # ensure we mark finalized if loop exits
            with active_algos_lock:
                if key in active_algos and not active_algos[key].get('finalized'):
                    active_algos[key]['finalized'] = True
                    active_algos[key]['finalized_at'] = int(time.time()*1000)
                    active_algos[key]['finalized_reason'] = 'terminated'
    t = threading.Thread(target=_thread, daemon=True)
    t.start()
    return t

# --------------------
# Place market entry and monitor (main flow) - updated to use algo endpoints and Decimal formatting
# --------------------

def place_market_entry_and_monitor(side: str, qty: float, entry_cs: int, tp_price: float, sl_price: float, signal_id: str, planned_limit_raw_close: float = None):
    def _thread():
        now_ms = int(time.time() * 1000)
        wait_ms = entry_cs - now_ms
        if wait_ms > 0:
            if VERBOSE: print(f"[SCHEDULE] Waiting {wait_ms/1000:.1f}s until entry_cs {entry_cs}")
            time.sleep(wait_ms/1000.0)

        side_uc = 'BUY' if side == 'long' else 'SELL'
        qty_adj = round_qty_floor(qty)
        if qty_adj <= 0:
            print('[ENTRY] computed qty <= 0, skipping')
            return

        dedupe_key = f"{entry_cs}-{planned_limit_raw_close}-{side_uc}"
        with scheduled_signals_set_lock:
            if dedupe_key in scheduled_signals_set:
                if VERBOSE:
                    print(f"[SCHEDULE] signal already scheduled, skipping duplicate for key={dedupe_key}")
                return
            scheduled_signals_set.add(dedupe_key)

        res = place_market_order_rest(side_uc, float(qty_adj))
        if res is None:
            print('[ENTRY] market order failed for signal', signal_id)
            with scheduled_signals_set_lock:
                scheduled_signals_set.discard(dedupe_key)
            return

        if VERBOSE:
            print('[ENTRY] order response', res)

        order_id = str(res.get('orderId') or '')
        if order_id:
            with order_to_signal_lock:
                order_to_signal[order_id] = signal_id

        fill_evt = wait_for_fill_for_order(order_id, wait_total_s=12.0, poll_interval_s=0.5)
        filled_qty = Decimal('0')
        entry_fill_price = None
        if fill_evt:
            try:
                filled_qty = Decimal(str(fill_evt.get('executedQty', 0.0) or 0.0))
            except Exception:
                filled_qty = Decimal('0')
            entry_fill_price = fill_evt.get('avgPrice') or None
            if (not entry_fill_price) or float(entry_fill_price) == 0.0:
                with agg_lock:
                    all_cs = sorted(agg_trades.keys())
                    if all_cs:
                        last_cs = all_cs[-1]
                        if agg_trades.get(last_cs):
                            entry_fill_price = float(agg_trades[last_cs][-1]['price'])
            if entry_fill_price is None:
                entry_fill_price = float(planned_limit_raw_close) if planned_limit_raw_close is not None else 0.0

        # authoritative positionAmt check (fix filled_qty mismatch)
        pos_amt = get_position_fill_qty_from_api(SYMBOL_NO_SLASH)
        if pos_amt is not None and pos_amt > 0:
            # use position amount as authoritative filled qty
            # round down to lot step
            pos_amt_q = round_qty_floor(float(pos_amt))
            if pos_amt_q > 0:
                filled_qty = pos_amt_q
                if VERBOSE:
                    print(f"[ENTRY-FILLED] using positionRisk qty {format_decimal_fixed(filled_qty)} as authoritative")

        if not filled_qty or float(filled_qty) <= 0:
            print('[ENTRY] No filled quantity detected for order', order_id, 'signal', signal_id, '— not placing TP/SL')
            with scheduled_signals_set_lock:
                scheduled_signals_set.discard(dedupe_key)
            return

        entry_fill_price = float(entry_fill_price)

        srow = {
            'signal_id': signal_id,
            'signal_time': ensure_ts_utc().isoformat(),
            'side': side,
            'entry_price': entry_fill_price,
            'limit_price': planned_limit_raw_close,
            'entry_fill_price': entry_fill_price,
            'entry_fill_ts': int(time.time() * 1000),
            'qty': float(filled_qty),
            'stop_loss': sl_price,
            'take_profit': tp_price,
            'entry_time': ensure_ts_utc().isoformat(),
            'simulated': False
        }
        signals_cache.append(srow)

        # recompute TP/SL points using TP_MAX_POINTS logic
        TP_POINTS = float(globals().get('TP_MAX_POINTS', TP_MAX_POINTS))
        TP_R_LOCAL = float(globals().get('TP_R', TP_R))
        SL_POINTS_LOCAL = TP_POINTS / TP_R_LOCAL if TP_R_LOCAL != 0 else TP_POINTS
        if side == 'long':
            recomputed_tp = entry_fill_price + TP_POINTS
            recomputed_sl = entry_fill_price - SL_POINTS_LOCAL
            close_side = 'SELL'
        else:
            recomputed_tp = entry_fill_price - TP_POINTS
            recomputed_sl = entry_fill_price + SL_POINTS_LOCAL
            close_side = 'BUY'

        # quantize prices and qty
        tp_dec = round_price_tick(recomputed_tp)
        sl_dec = round_price_tick(recomputed_sl)
        qty_for_orders = round_qty_floor(float(filled_qty))

        if qty_for_orders <= 0:
            print('[TP/SL] floored qty <= 0 after rounding; skipping TP/SL for signal', signal_id)
            with scheduled_signals_set_lock:
                scheduled_signals_set.discard(dedupe_key)
            return

        # prepare clientAlgoId safe short strings
        client_tp = f"tp-{signal_id[:12]}"
        client_sl = f"sl-{signal_id[:12]}"

        # place algos: use CONDITIONAL algos (TAKE_PROFIT_MARKET and STOP_MARKET)
        tp_resp = place_algo_order('CONDITIONAL', 'TAKE_PROFIT_MARKET', close_side, tp_dec, qty_for_orders, client_tp, max_retries=3, signal_id=signal_id)
        sl_resp = place_algo_order('CONDITIONAL', 'STOP_MARKET', close_side, sl_dec, qty_for_orders, client_sl, max_retries=3, signal_id=signal_id)

        tp_algo = {}
        sl_algo = {}
        if tp_resp:
            tp_algo['algoId'] = str(tp_resp.get('algoId'))
            tp_algo['clientAlgoId'] = tp_resp.get('clientAlgoId')
            tp_algo['triggerPrice'] = float(tp_resp.get('triggerPrice') or tp_dec)
            tp_algo['status'] = tp_resp.get('algoStatus')
        else:
            tp_algo['clientAlgoId'] = client_tp
            tp_algo['status'] = 'placement_failed'

        if sl_resp:
            sl_algo['algoId'] = str(sl_resp.get('algoId'))
            sl_algo['clientAlgoId'] = sl_resp.get('clientAlgoId')
            sl_algo['triggerPrice'] = float(sl_resp.get('triggerPrice') or sl_dec)
            sl_algo['status'] = sl_resp.get('algoStatus')
        else:
            sl_algo['clientAlgoId'] = client_sl
            sl_algo['status'] = 'placement_failed'

        with active_algos_lock:
            active_algos[signal_id] = {'tp': tp_algo, 'sl': sl_algo, 'finalized': False, 'placed_at': int(time.time()*1000)}

        if tp_resp or sl_resp:
            if VERBOSE:
                print(f"[TP/SL-ALGO] placed for signal={signal_id} algo_info={{'tp':{tp_algo}, 'sl':{sl_algo}}}")
            # start watcher thread to monitor and perform OCO
            start_algo_watcher(signal_id, tp_algo, sl_algo)
            # remove dedupe key but keep signals_cache record
            with scheduled_signals_set_lock:
                scheduled_signals_set.discard(dedupe_key)
            return

        # if both placements failed, fallback to market close as last resort
        print(f"[EXIT-ORDERS] native TP/SL placement failed after retries for signal={signal_id}; executing fallback market-close")
        try:
            close_side = 'SELL' if side == 'long' else 'BUY'
            q_close = float(qty_for_orders)
            if q_close <= 0:
                print('[FALLBACK] cannot market-close; qty <= 0')
            else:
                close_res = place_market_order_rest(close_side, q_close)
                if VERBOSE:
                    print('[FALLBACK] close order response', close_res)
        except Exception as e:
            print('[FALLBACK] market close failed', repr(e))
        finally:
            with scheduled_signals_set_lock:
                scheduled_signals_set.discard(dedupe_key)
            return

    t = threading.Thread(target=_thread, daemon=True)
    t.start()
    return t

# --------------------
# In-memory signal lookup
# --------------------

def find_existing_signal_in_memory(entry_cs, planned_limit_raw_close):
    try:
        for s in signals_cache:
            try:
                s_entry = float(s.get('entry_cs', 0))
                s_limit = float(s.get('planned_limit_raw_close', s.get('limit_price', 0) or 0))
                if float(entry_cs) == s_entry and float(planned_limit_raw_close) == s_limit:
                    return s
            except Exception:
                continue
    except Exception:
        pass
    return None

# --------------------
# Finalizer and scheduling
# --------------------

def finalize_previous_and_run_once():
    global df, signals_cache, EQUITY
    if df.empty:
        return
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    current_cs = candle_start_for_ts(now_ms)
    prev_cs = current_cs - TF_HOURS * 3600 * 1000

    with agg_lock:
        if prev_cs not in agg_totals:
            return
        b_base = agg_totals[prev_cs]['buy_base']
        s_base = agg_totals[prev_cs]['sell_base']
        b_quote = agg_totals[prev_cs]['buy_quote']
        s_quote = agg_totals[prev_cs]['sell_quote']

    idxs = df.index[df['ts_ms'] == prev_cs]
    if idxs.empty:
        try:
            kl = fetch_klines_demo(SYMBOL_NO_SLASH, TIMEFRAME, limit=1, end_time=prev_cs + 1)
            if kl:
                r = kl[0]
                new_ts = int(r[0])
                new_row = pd.DataFrame([{'timestamp': pd.to_datetime(new_ts, unit='ms', utc=True), 'ts_ms': new_ts,
                                         'open': float(r[1]), 'high': float(r[2]), 'low': float(r[3]), 'close': float(r[4]), 'volume': float(r[5])}])
                new_row = new_row.set_index('timestamp')
                try:
                    df = pd.concat([df, new_row], axis=0).sort_index()
                except Exception:
                    pass
        except Exception as e:
            print('finalizer: REST candle fetch failed:', repr(e))
            return
    idxs = df.index[df['ts_ms'] == prev_cs]
    if idxs.empty:
        return
    idx = idxs[0]

    with agg_lock:
        df.at[idx, 'buy_vol_base'] = b_base
        df.at[idx, 'sell_vol_base'] = s_base
        df.at[idx, 'delta_base_raw'] = b_base - s_base
        df.at[idx, 'buy_vol_quote'] = b_quote
        df.at[idx, 'sell_vol_quote'] = s_quote
        df.at[idx, 'delta_quote_raw'] = b_quote - s_quote
        total_base = (b_base + s_base) if (b_base + s_base) > 0 else 1e-12
        df.at[idx, 'delta_base_pct'] = (b_base - s_base) / total_base
        df.at[idx, 'delta'] = df.at[idx, 'delta_base_raw']
        df['prev_close'] = df['close'].shift(1)
        df['tr'] = df[['high','low','prev_close']].apply(lambda r: max(r['high']-r['low'], abs(r['high']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['high'])), abs(r['low']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['low']))), axis=1)
        df['atr'] = df['tr'].rolling(14, min_periods=1).mean().bfill()

    sr = compute_sr_zones_safe(df, n1=3, n2=2, dedupe_pct=0.003, zone_buffer_pct=0.002)
    support_zones = sr['support_zones']; resistance_zones = sr['resistance_zones']

    scheduled = []
    try:
        scheduled = detect_signals_on_df_and_schedule(df, support_zones, resistance_zones)
    except Exception as e:
        print('Error during detect_signals_on_df_and_schedule:', repr(e))
        traceback.print_exc()
        return

    if not scheduled:
        if VERBOSE:
            print('No signals at', pd.to_datetime(prev_cs, unit='ms', utc=True))
        return

    if VERBOSE:
        print(f"Detected {len(scheduled)} scheduled entries at {pd.to_datetime(prev_cs, unit='ms', utc=True)}")
    for s in scheduled:
        try:
            dedupe_key = f"{s['entry_cs']}-{s['planned_limit_raw_close']}-{s.get('side','')}"
            with scheduled_signals_set_lock:
                if dedupe_key in scheduled_signals_set:
                    if VERBOSE:
                        print(f"[SCHEDULE] signal already exists, skipping {s['entry_cs']} {s['planned_limit_raw_close']}")
                    continue
                scheduled_signals_set.add(dedupe_key)

            equity_usd = EQUITY
            try:
                bal = _signed_get('/fapi/v2/account')
                equity_usd = float(bal.get('availableBalance') or bal.get('totalWalletBalance') or EQUITY)
            except Exception:
                equity_usd = EQUITY

            if s['side'] == 'long':
                stop_price = s['planned_limit_raw_close'] - s['stop_points']
                tp_price = s['planned_limit_raw_close'] + s['tp_points']
                entry_price_est = s['planned_limit_raw_close']
            else:
                stop_price = s['planned_limit_raw_close'] + s['stop_points']
                tp_price = s['planned_limit_raw_close'] - s['tp_points']
                entry_price_est = s['planned_limit_raw_close']

            qty, dollar_risk, notional, size_capped = compute_qty_from_risk_usd_live(entry_price_est, stop_price, equity_usd or EQUITY, RISK_PER_TRADE)
            signal_id = str(uuid.uuid4())

            existing = find_existing_signal_in_memory(s['entry_cs'], s['planned_limit_raw_close'])
            if existing:
                if VERBOSE:
                    print(f"[SCHEDULE] signal already exists (in-memory), skipping {s['entry_cs']} {s['planned_limit_raw_close']}")
                with scheduled_signals_set_lock:
                    scheduled_signals_set.discard(dedupe_key)
                continue

            srow = {'signal_id': signal_id, 'side': s['side'], 'entry_cs': s['entry_cs'], 'limit_price': s['limit_price'], 'planned_limit_raw_close': s['planned_limit_raw_close'], 'qty': qty, 'dollar_risk': dollar_risk, 'notional': notional, 'tp_price': tp_price, 'stop_price': stop_price}
            signals_cache.append(srow)
            place_market_entry_and_monitor(s['side'], qty, s['entry_cs'], tp_price, stop_price, signal_id, planned_limit_raw_close=s['planned_limit_raw_close'])
            if VERBOSE:
                print(f"[SCHEDULED] signal_id={signal_id} side={s['side']} entry_cs={pd.to_datetime(s['entry_cs'], unit='ms', utc=True)} qty={qty} tp={tp_price} sl={stop_price}")
        except Exception as e:
            print('[SCHEDULE] scheduling error', repr(e))
            traceback.print_exc()
            try:
                with scheduled_signals_set_lock:
                    scheduled_signals_set.discard(dedupe_key)
            except Exception:
                pass

# --------------------
# Watchdog & startup
# --------------------

def _safe_restart_agg():
    global listener_thread
    try:
        if listener_thread is not None and listener_thread.is_alive():
            return True
        print('[WATCHDOG] restarting agg listener...')
        listener_thread = start_agg_listener_in_thread()
        time.sleep(2)
        return listener_thread is not None and listener_thread.is_alive()
    except Exception:
        print('[WATCHDOG] restart agg error', traceback.format_exc())
        return False

def _safe_restart_finalizer():
    global finalizer_thread
    try:
        if finalizer_thread is not None and finalizer_thread.is_alive():
            return True
        print('[WATCHDOG] restarting finalizer...')
        finalizer_thread = threading.Thread(target=finalizer_loop, daemon=True)
        finalizer_thread.start()
        time.sleep(2)
        return finalizer_thread is not None and finalizer_thread.is_alive()
    except Exception:
        print('[WATCHDOG] restart finalizer exception', traceback.format_exc())
        return False

def watchdog_loop():
    while True:
        try:
            ok1 = listener_thread is not None and listener_thread.is_alive()
            ok2 = finalizer_thread is not None and finalizer_thread.is_alive()
            if not ok1:
                _safe_restart_agg()
            if not ok2:
                _safe_restart_finalizer()
        except Exception:
            print('[WATCHDOG] exception', traceback.format_exc())
        time.sleep(max(5, WATCHDOG_POLL_SEC))

# --------------------
# Finalizer thread wrapper
# --------------------

def finalizer_loop(poll_interval=20):
    while True:
        try:
            finalize_previous_and_run_once()
        except Exception:
            print('finalizer exception', traceback.format_exc())
        time.sleep(poll_interval)

# --------------------
# Flask diagnostics endpoints
# --------------------

@app.route('/health')
def health():
    return jsonify({'status':'ok','time': datetime.now(timezone.utc).isoformat()})

@app.route('/diag')
def diag_route():
    try:
        diag = {}
        diag['listener_running'] = (listener_thread is not None and listener_thread.is_alive())
        diag['finalizer_running'] = (finalizer_thread is not None and finalizer_thread.is_alive())
        diag['user_ws_running'] = (user_ws_thread is not None and user_ws_thread.is_alive())
        diag['df_len'] = len(df)
        if not df.empty:
            tail = df[['ts_ms','open','high','low','close','volume']].tail(5).to_dict(orient='records')
            diag['df_tail'] = tail
        else:
            diag['df_tail'] = []
        try:
            sr = compute_sr_zones_safe(df, n1=3, n2=2, dedupe_pct=0.003, zone_buffer_pct=0.002)
            diag['supports_count'] = len(sr.get('support_zones', []))
            diag['resistances_count'] = len(sr.get('resistance_zones', []))
        except Exception:
            diag['supports_count'] = 0
            diag['resistances_count'] = 0
        diag['recent_signals'] = signals_cache[-20:]
        with listen_key_lock:
            diag['listen_key'] = True if listen_key else False
        try:
            bal = _signed_get('/fapi/v2/account')
            diag['availableBalance'] = float(bal.get('availableBalance') or bal.get('totalWalletBalance') or 0)
        except Exception:
            diag['availableBalance'] = None
        with active_algos_lock:
            diag['active_algos'] = active_algos
        return jsonify(diag)
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/sr')
def sr_route():
    try:
        sr = compute_sr_zones_safe(df, n1=3, n2=2, dedupe_pct=0.003, zone_buffer_pct=0.002)
        out = {
            'supports': sr.get('support_zones', [])[:50],
            'resistances': sr.get('resistance_zones', [])[:50]
        }
        return jsonify(out)
    except Exception as e:
        return jsonify({'error': str(e)})

def start_flask_thread(port: int = 5000):
    def _run():
        print(f'[DIAG] HTTP diagnostics started on 127.0.0.1:{port}')
        app.run(host='127.0.0.1', port=port, debug=False, use_reloader=False)
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t

# --------------------
# Startup
# --------------------

def start_background():
    global listener_thread, finalizer_thread, watchdog_thread, user_ws_thread
    listener_thread = start_agg_listener_in_thread()
    finalizer_thread = threading.Thread(target=finalizer_loop, daemon=True)
    finalizer_thread.start()
    create_listen_key()
    user_ws_thread = start_user_ws_in_thread()
    t_keep = threading.Thread(target=keepalive_listen_key_loop, daemon=True)
    t_keep.start()
    watchdog_thread = threading.Thread(target=watchdog_loop, daemon=True)
    watchdog_thread.start()
    print('Background threads started.')


def show_startup_diagnostics():
    print('\n=== STARTUP DIAGNOSTICS ===')
    try:
        set_leverage_for_symbol(SYMBOL_NO_SLASH, LEVERAGE)
    except Exception:
        pass
    try:
        bal = _signed_get('/fapi/v2/account')
        avail = float(bal.get('availableBalance') or bal.get('totalWalletBalance') or 0)
        print('[DIAG] demo account availableBalance=', avail)
    except Exception as e:
        print('[DIAG] Could not fetch account balance. Exception:', repr(e))
    try:
        init_exchange_info()
        if df is None or df.empty:
            print('[DIAG] df is empty, no SR zones.')
        else:
            sr = compute_sr_zones_safe(df, n1=3, n2=2, dedupe_pct=0.003, zone_buffer_pct=0.002)
            s_count = len(sr.get('support_zones', [])); r_count = len(sr.get('resistance_zones', []))
            print(f'[DIAG] SR zones computed: supports={s_count}, resistances={r_count}')
    except Exception as e:
        print('[DIAG] SR computation failed:', repr(e))
    print('=== END STARTUP DIAGNOSTICS ===\n')


def start():
    init_exchange_info()
    init_warmup_klines()
    show_startup_diagnostics()
    start_background()
    start_flask_thread(int(os.getenv('DIAG_PORT', '5000')))

if __name__ == '__main__':
    start()
    while True:
        time.sleep(10)
