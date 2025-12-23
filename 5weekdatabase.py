import yfinance as yf
import datetime
import duckdb
import numpy as np
import pytz

from databasefunctions import (
    get_closest_strike,
    get_option_quote,
    compute_z_scores_for_bucket,  # assume this is wired to use option_snapshots_5w
)

est = pytz.timezone("America/New_York")


def py(x):
    # convert numpy numbers → plain Python types
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        return float(x)
    return x


# ---------- STEP 1: Get this week's Friday chain ----------

now = datetime.datetime.now()
print(now.strftime("%Y-%m-%d %H:%M"))
now_dateobject = now.date()

stock = yf.Ticker("AAPL")  # replace "AAPL" with whatever ticker you want

# get list of expiration dates available
expirations = stock.options


def get_friday_within_4_days():
    for exp in expirations:
        d = datetime.datetime.strptime(exp, "%Y-%m-%d").date()
        if d.weekday() == 4 and (d - now_dateobject).days <= 4:
            return exp   # ✅ return the Friday expiration
    return None          # ✅ means: no Friday in range


exp = get_friday_within_4_days()

if exp is None:
    print("No valid Friday expiration in range.")
    exit()   # ← stop the entire script right here

chain = stock.option_chain(exp)
calls = chain.calls
puts = chain.puts

# current price
atm = stock.info["currentPrice"]

# 1–2% and 3–4% OTM targets
otm_call_1_strike = atm * 1.015
otm_put_1_strike  = atm * 0.985

otm_call_2_strike = atm * 1.035
otm_put_2_strike  = atm * 0.965

# ---------- STEP 2: Find closest strikes using helper ----------

closest_atm_call = get_closest_strike(chain, "C", atm)
closest_atm_put  = get_closest_strike(chain, "P", atm)

otm_call_1_closest = get_closest_strike(chain, "C", otm_call_1_strike)
otm_put_1_closest  = get_closest_strike(chain, "P", otm_put_1_strike)

otm_call_2_closest = get_closest_strike(chain, "C", otm_call_2_strike)
otm_put_2_closest  = get_closest_strike(chain, "P", otm_put_2_strike)



atm_call_option_strike_OCC = f"{int(closest_atm_call * 1000):08d}"
atm_put_option_strike_OCC  = f"{int(closest_atm_put * 1000):08d}"

otm1_call_option_strike_OCC = f"{int(otm_call_1_closest * 1000):08d}"
otm1_put_option_strike_OCC  = f"{int(otm_put_1_closest * 1000):08d}"

otm2_call_option_strike_OCC = f"{int(otm_call_2_closest * 1000):08d}"
otm2_put_option_strike_OCC  = f"{int(otm_put_2_closest * 1000):08d}"



# ---------- STEP 3: Pull option quotes using helper ----------

# ATM CALL
atm_call_q = get_option_quote(chain, "C", closest_atm_call)
atm_call_bid         = atm_call_q["bid"]
atm_call_ask         = atm_call_q["ask"]
atm_call_mid         = atm_call_q["mid"]
atm_call_volume      = atm_call_q["volume"]
atm_call_iv          = atm_call_q["iv"]
atm_call_oi          = atm_call_q["oi"]
atm_call_spread      = atm_call_q["spread"]
atm_call_spread_pct  = atm_call_q["spread_pct"]

# ATM PUT
atm_put_q = get_option_quote(chain, "P", closest_atm_put)
atm_put_bid         = atm_put_q["bid"]
atm_put_ask         = atm_put_q["ask"]
atm_put_mid         = atm_put_q["mid"]
atm_put_volume      = atm_put_q["volume"]
atm_put_iv          = atm_put_q["iv"]
atm_put_oi          = atm_put_q["oi"]
atm_put_spread      = atm_put_q["spread"]
atm_put_spread_pct  = atm_put_q["spread_pct"]

# OTM_1 CALL
otm1_call_q = get_option_quote(chain, "C", otm_call_1_closest)
otm_call_1_bid        = otm1_call_q["bid"]
otm_call_1_ask        = otm1_call_q["ask"]
otm_call_1_mid        = otm1_call_q["mid"]
otm_call_1_volume     = otm1_call_q["volume"]
otm_call_1_iv         = otm1_call_q["iv"]
otm_call_1_oi         = otm1_call_q["oi"]
otm_call_1_spread     = otm1_call_q["spread"]
otm_call_1_spread_pct = otm1_call_q["spread_pct"]

# OTM_1 PUT
otm1_put_q = get_option_quote(chain, "P", otm_put_1_closest)
otm_put_1_bid        = otm1_put_q["bid"]
otm_put_1_ask        = otm1_put_q["ask"]
otm_put_1_mid        = otm1_put_q["mid"]
otm_put_1_volume     = otm1_put_q["volume"]
otm_put_1_iv         = otm1_put_q["iv"]
otm_put_1_oi         = otm1_put_q["oi"]
otm_put_1_spread     = otm1_put_q["spread"]
otm_put_1_spread_pct = otm1_put_q["spread_pct"]

# OTM_2 CALL
otm2_call_q = get_option_quote(chain, "C", otm_call_2_closest)
otm_call_2_bid        = otm2_call_q["bid"]
otm_call_2_ask        = otm2_call_q["ask"]
otm_call_2_mid        = otm2_call_q["mid"]
otm_call_2_volume     = otm2_call_q["volume"]
otm_call_2_iv         = otm2_call_q["iv"]
otm_call_2_oi         = otm2_call_q["oi"]
otm_call_2_spread     = otm2_call_q["spread"]
otm_call_2_spread_pct = otm2_call_q["spread_pct"]

# OTM_2 PUT
otm2_put_q = get_option_quote(chain, "P", otm_put_2_closest)
otm_put_2_bid        = otm2_put_q["bid"]
otm_put_2_ask        = otm2_put_q["ask"]
otm_put_2_mid        = otm2_put_q["mid"]
otm_put_2_volume     = otm2_put_q["volume"]
otm_put_2_iv         = otm2_put_q["iv"]
otm_put_2_oi         = otm2_put_q["oi"]
otm_put_2_spread     = otm2_put_q["spread"]
otm_put_2_spread_pct = otm2_put_q["spread_pct"]

# ---------- STEP 4: Timestamp / buckets ----------

now_est = datetime.datetime.now(est)
timestamp = now_est.strftime("%Y-%m-%d %H:%M:%S")

symbol = stock.ticker
expiration = exp  # string "YYYY-MM-DD"
snapshot_id = f"{symbol}_{timestamp}"

exp_date = datetime.datetime.strptime(expiration, "%Y-%m-%d").date()
days_till_expiry = (exp_date - now_dateobject).days

if days_till_expiry <= 1:
    time_decay_bucket = "EXTREME"
elif days_till_expiry <= 3:
    time_decay_bucket = "HIGH"
elif days_till_expiry <= 7:
    time_decay_bucket = "MEDIUM"
else:
    time_decay_bucket = "LOW"


option_symbol_atm_call = f"{symbol}{exp_date}C{atm_call_option_strike_OCC}"
option_symbol_atm_put  = f"{symbol}{exp_date}P{atm_put_option_strike_OCC}"

option_symbol_otm1_call = f"{symbol}{exp_date}C{otm1_call_option_strike_OCC}"
option_symbol_otm1_put  = f"{symbol}{exp_date}P{otm1_put_option_strike_OCC}"

option_symbol_otm2_call = f"{symbol}{exp_date}C{otm2_call_option_strike_OCC}"
option_symbol_otm2_put  = f"{symbol}{exp_date}P{otm2_put_option_strike_OCC}"




# ---------- STEP 5: Connect to DB & write raw 5w snapshots ----------

con = duckdb.connect("options_data.db")



con.execute("""
CREATE TABLE IF NOT EXISTS option_snapshots_5w (
    timestamp TIMESTAMP,
    call_put TEXT,
    moneyness_bucket TEXT,
    time_decay_bucket TEXT,
    mid DOUBLE,
    volume INTEGER,
    iv DOUBLE
);
""")




con.execute("""
DELETE FROM option_snapshots_5w
WHERE timestamp < NOW() - INTERVAL '35 days';
""")

con.execute("""
INSERT INTO option_snapshots_5w (
    timestamp,
    call_put,
    moneyness_bucket,
    time_decay_bucket,
    mid,
    volume,
    iv
)
VALUES
    (?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?);
""", [py(x) for x in [
    # ATM CALL
    timestamp, "C", "ATM",   time_decay_bucket, atm_call_mid, atm_call_volume, atm_call_iv,

    # ATM PUT
    timestamp, "P", "ATM",   time_decay_bucket, atm_put_mid, atm_put_volume, atm_put_iv,

    # OTM CALL 1
    timestamp, "C", "OTM_1", time_decay_bucket, otm_call_1_mid, otm_call_1_volume, otm_call_1_iv,

    # OTM PUT 1
    timestamp, "P", "OTM_1", time_decay_bucket, otm_put_1_mid, otm_put_1_volume, otm_put_1_iv,

    # OTM CALL 2
    timestamp, "C", "OTM_2", time_decay_bucket, otm_call_2_mid, otm_call_2_volume, otm_call_2_iv,

    # OTM PUT 2
    timestamp, "P", "OTM_2", time_decay_bucket, otm_put_2_mid, otm_put_2_volume, otm_put_2_iv,
]])



# ---------- STEP 6: Compute Z-scores using helper (5w history) ----------

atm_call_z, atm_call_vol_z, atm_call_iv_z = compute_z_scores_for_bucket(
    con,
    bucket="ATM",
    call_put="C",
    time_decay_bucket=time_decay_bucket,
    current_mid=atm_call_mid,
    current_volume=atm_call_volume,
    current_iv=atm_call_iv,
)

atm_put_z, atm_put_vol_z, atm_put_iv_z = compute_z_scores_for_bucket(
    con,
    bucket="ATM",
    call_put="P",
    time_decay_bucket=time_decay_bucket,
    current_mid=atm_put_mid,
    current_volume=atm_put_volume,
    current_iv=atm_put_iv,
)

otm_call_1_z, otm_call_1_vol_z, otm_call_1_iv_z = compute_z_scores_for_bucket(
    con,
    bucket="OTM_1",
    call_put="C",
    time_decay_bucket=time_decay_bucket,
    current_mid=otm_call_1_mid,
    current_volume=otm_call_1_volume,
    current_iv=otm_call_1_iv,
)

otm_put_1_z, otm_put_1_vol_z, otm_put_1_iv_z = compute_z_scores_for_bucket(
    con,
    bucket="OTM_1",
    call_put="P",
    time_decay_bucket=time_decay_bucket,
    current_mid=otm_put_1_mid,
    current_volume=otm_put_1_volume,
    current_iv=otm_put_1_iv,
)

otm_call_2_z, otm_call_2_vol_z, otm_call_2_iv_z = compute_z_scores_for_bucket(
    con,
    bucket="OTM_2",
    call_put="C",
    time_decay_bucket=time_decay_bucket,
    current_mid=otm_call_2_mid,
    current_volume=otm_call_2_volume,
    current_iv=otm_call_2_iv,
)

otm_put_2_z, otm_put_2_vol_z, otm_put_2_iv_z = compute_z_scores_for_bucket(
    con,
    bucket="OTM_2",
    call_put="P",
    time_decay_bucket=time_decay_bucket,
    current_mid=otm_put_2_mid,
    current_volume=otm_put_2_volume,
    current_iv=otm_put_2_iv,
)

# ---------- STEP 7: Insert into enriched 5w table ----------


con.execute("""
CREATE TABLE IF NOT EXISTS option_snapshots_enriched_5w (
    snapshot_id TEXT,
    timestamp TIMESTAMP,
    symbol TEXT,
    option_symbol TEXT,          -- <-- ADDED
    strike DOUBLE,
    call_put TEXT,
    days_to_expiry INTEGER,
    expiration_date DATE,
    moneyness_bucket TEXT,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    volume INTEGER,
    open_interest INTEGER,
    iv DOUBLE,
    spread DOUBLE,
    spread_pct DOUBLE,
    time_decay_bucket TEXT,
    mid_z DOUBLE,
    volume_z DOUBLE,
    iv_z DOUBLE,

    -- ==== SIGNAL LOGGING (BOOLEAN) ====
    atm_call_signal BOOLEAN,
    atm_put_signal  BOOLEAN,
    otm1_call_signal BOOLEAN,
    otm1_put_signal  BOOLEAN,
    otm2_call_signal BOOLEAN,
    otm2_put_signal  BOOLEAN,

    -- ==== RETURN LABELS ====
    opt_ret_10m DOUBLE,
    opt_ret_1h DOUBLE,
    opt_ret_eod DOUBLE,
    opt_ret_next_open DOUBLE,
    opt_ret_1d DOUBLE,
    opt_ret_exp DOUBLE
);
""")

con.execute("""
DELETE FROM option_snapshots_enriched_5w
WHERE timestamp < NOW() - INTERVAL '35 days';
""")

con.execute("""
INSERT INTO option_snapshots_enriched_5w (
    snapshot_id,
    timestamp,
    symbol,
    option_symbol,
    strike,
    call_put,
    days_to_expiry,
    expiration_date,
    moneyness_bucket,
    bid,
    ask,
    mid,
    volume,
    open_interest,
    iv,
    spread,
    spread_pct,
    time_decay_bucket,
    mid_z,
    volume_z,
    iv_z,

    atm_call_signal,
    atm_put_signal,
    otm1_call_signal,
    otm1_put_signal,
    otm2_call_signal,
    otm2_put_signal,

    opt_ret_10m,
    opt_ret_1h,
    opt_ret_eod,
    opt_ret_next_open,
    opt_ret_1d,
    opt_ret_exp
)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL),

    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL),

    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL),

    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL),

    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL),

    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
     NULL, NULL, NULL, NULL, NULL, NULL,
     NULL, NULL, NULL, NULL, NULL, NULL)
""", [py(x) for x in [

    # ===== ATM CALL =====
    snapshot_id, timestamp, symbol, option_symbol_atm_call, closest_atm_call, "C",
    days_till_expiry, exp_date,
    "ATM",
    atm_call_bid, atm_call_ask, atm_call_mid,
    atm_call_volume, atm_call_oi,
    atm_call_iv, atm_call_spread, atm_call_spread_pct,
    time_decay_bucket,
    atm_call_z, atm_call_vol_z, atm_call_iv_z,

    # ===== ATM PUT =====
    snapshot_id, timestamp, symbol, option_symbol_atm_put, closest_atm_put, "P",
    days_till_expiry, exp_date,
    "ATM",
    atm_put_bid, atm_put_ask, atm_put_mid,
    atm_put_volume, atm_put_oi,
    atm_put_iv, atm_put_spread, atm_put_spread_pct,
    time_decay_bucket,
    atm_put_z, atm_put_vol_z, atm_put_iv_z,

    # ===== OTM CALL 1 =====
    snapshot_id, timestamp, symbol, option_symbol_otm1_call, otm_call_1_closest, "C",
    days_till_expiry, exp_date,
    "OTM_1",
    otm_call_1_bid, otm_call_1_ask, otm_call_1_mid,
    otm_call_1_volume, otm_call_1_oi,
    otm_call_1_iv, otm_call_1_spread, otm_call_1_spread_pct,
    time_decay_bucket,
    otm_call_1_z, otm_call_1_vol_z, otm_call_1_iv_z,

    # ===== OTM PUT 1 =====
    snapshot_id, timestamp, symbol, option_symbol_otm1_put, otm_put_1_closest, "P",
    days_till_expiry, exp_date,
    "OTM_1",
    otm_put_1_bid, otm_put_1_ask, otm_put_1_mid,
    otm_put_1_volume, otm_put_1_oi,
    otm_put_1_iv, otm_put_1_spread, otm_put_1_spread_pct,
    time_decay_bucket,
    otm_put_1_z, otm_put_1_vol_z, otm_put_1_iv_z,

    # ===== OTM CALL 2 =====
    snapshot_id, timestamp, symbol, option_symbol_otm2_call, otm_call_2_closest, "C",
    days_till_expiry, exp_date,
    "OTM_2",
    otm_call_2_bid, otm_call_2_ask, otm_call_2_mid,
    otm_call_2_volume, otm_call_2_oi,
    otm_call_2_iv, otm_call_2_spread, otm_call_2_spread_pct,
    time_decay_bucket,
    otm_call_2_z, otm_call_2_vol_z, otm_call_2_iv_z,

    # ===== OTM PUT 2 =====
    snapshot_id, timestamp, symbol, option_symbol_otm2_put, otm_put_2_closest, "P",
    days_till_expiry, exp_date,
    "OTM_2",
    otm_put_2_bid, otm_put_2_ask, otm_put_2_mid,
    otm_put_2_volume, otm_put_2_oi,
    otm_put_2_iv, otm_put_2_spread, otm_put_2_spread_pct,
    time_decay_bucket,
    otm_put_2_z, otm_put_2_vol_z, otm_put_2_iv_z
]])





con.close()

