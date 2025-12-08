import yfinance as yf
import datetime
import pandas as pd
import duckdb
from message import send_text
import numpy as np

def py(x):
    # convert numpy numbers → plain Python types
    if isinstance(x, (np.integer,)):
        return int(x)
    if isinstance(x, (np.floating,)):
        return float(x)
    return x


# ---------- STEP 1: Get this week's Friday chain ----------

today = datetime.date.today()

print(today)



stock = yf.Ticker("AAPL")  # replace "AAPL" with whatever ticker you want

# get list of expiration dates available
expirations = stock.options

def get_friday_within_4_days():
    for exp in expirations:
        d = datetime.datetime.strptime(exp, "%Y-%m-%d").date()

        if d.weekday() == 4 and (d - today).days <= 4:
            return exp   # ✅ return the Friday expiration

    return None  # ✅ means: no Friday in range


exp = get_friday_within_4_days()


if exp is None:
    print("No valid Friday expiration in range.")
    exit()   # ← stop the entire script right here

chain = stock.option_chain(exp)
calls = chain.calls
puts = chain.puts


#current price
atm = stock.info["currentPrice"]


#closest atm call
min_diff = float("inf")
closest_atm_call = None

for strike in chain.calls["strike"]:
    diff = abs(strike - atm)
    if diff < min_diff:
        min_diff = diff
        closest_atm_call = strike

#closest atm put
min_diff_put = float("inf")
closest_atm_put = None

for strike in chain.puts["strike"]:
    diff = abs(strike - atm)
    if diff < min_diff_put:
        min_diff_put = diff
        closest_atm_put = strike



#1-2 percent otm
otm_call_1_strike = atm * 1.015
otm_put_1_strike  = atm * 0.985

# 3–4% OTM
otm_call_2_strike = atm * 1.035
otm_put_2_strike  = atm * 0.965


otm_call_1_target = otm_call_1_strike

otm_call_1_min_diff = float("inf")
otm_call_1_closest = None
#gets closest strike to 1-2 percent otm call
for strike in chain.calls["strike"]:
    diff = abs(strike - otm_call_1_target)
    if diff < otm_call_1_min_diff:
        otm_call_1_min_diff = diff
        otm_call_1_closest = strike

print(otm_call_1_closest)

#gets closest strike to 1-2 percent otm put
otm_put_1_target = otm_put_1_strike

otm_put_1_min_diff = float("inf")
otm_put_1_closest = None

for strike in chain.puts["strike"]:
    diff = abs(strike - otm_put_1_target)
    if diff < otm_put_1_min_diff:
        otm_put_1_min_diff = diff
        otm_put_1_closest = strike


print(otm_put_1_closest)


#gets closest strike to 3-4 percent otm call
otm_call_2_target = otm_call_2_strike

otm_call_2_min_diff = float("inf")
otm_call_2_closest = None

for strike in chain.calls["strike"]:
    diff = abs(strike - otm_call_2_target)
    if diff < otm_call_2_min_diff:
        otm_call_2_min_diff = diff
        otm_call_2_closest = strike


#gets closest strike to 3-4 percent otm put
otm_put_2_target = otm_put_2_strike

otm_put_2_min_diff = float("inf")
otm_put_2_closest = None

for strike in chain.puts["strike"]:
    diff = abs(strike - otm_put_2_target)
    if diff < otm_put_2_min_diff:
        otm_put_2_min_diff = diff
        otm_put_2_closest = strike


#gets the price in chain closest to estimated strike 
atm_call_price = chain.calls[ chain.calls["strike"] == closest_atm_call ]["lastPrice"].iloc[0]

atm_put_price = chain.puts[ chain.puts["strike"] == closest_atm_put ]["lastPrice"].iloc[0]


otm_call_1_price = chain.calls[ chain.calls["strike"] == otm_call_1_closest ]["lastPrice"].iloc[0]
otm_call_2_price = chain.calls[ chain.calls["strike"] == otm_call_2_closest ]["lastPrice"].iloc[0]

otm_put_1_price  = chain.puts[ chain.puts["strike"] == otm_put_1_closest ]["lastPrice"].iloc[0]
otm_put_2_price  = chain.puts[ chain.puts["strike"] == otm_put_2_closest ]["lastPrice"].iloc[0]

#getting bid prices
atm_call_bid = chain.calls[ chain.calls["strike"] == closest_atm_call ]["bid"].iloc[0]
atm_put_bid = chain.puts[ chain.puts["strike"] == closest_atm_put ]["bid"].iloc[0]

otm_call_1_bid = chain.calls[ chain.calls["strike"] == otm_call_1_closest ]["bid"].iloc[0]
otm_call_2_bid = chain.calls[ chain.calls["strike"] == otm_call_2_closest ]["bid"].iloc[0]

otm_put_1_bid  = chain.puts[ chain.puts["strike"] == otm_put_1_closest ]["bid"].iloc[0]
otm_put_2_bid  = chain.puts[ chain.puts["strike"] == otm_put_2_closest ]["bid"].iloc[0]

#getting ask prices
atm_call_ask = chain.calls[ chain.calls["strike"] == closest_atm_call ]["ask"].iloc[0]
atm_put_ask = chain.puts[ chain.puts["strike"] == closest_atm_put ]["ask"].iloc[0]

otm_call_1_ask = chain.calls[ chain.calls["strike"] == otm_call_1_closest ]["ask"].iloc[0]
otm_call_2_ask = chain.calls[ chain.calls["strike"] == otm_call_2_closest ]["ask"].iloc[0]


otm_put_1_ask  = chain.puts[ chain.puts["strike"] == otm_put_1_closest ]["ask"].iloc[0]
otm_put_2_ask  = chain.puts[ chain.puts["strike"] == otm_put_2_closest ]["ask"].iloc[0]
#getting mids
atm_call_mid = (atm_call_bid + atm_call_ask) / 2
atm_put_mid = (atm_put_bid + atm_put_ask) / 2


otm_call_1_mid = (otm_call_1_bid + otm_call_1_ask) / 2
otm_call_2_mid = (otm_call_2_bid + otm_call_2_ask) / 2


otm_put_1_mid = (otm_put_1_bid + otm_put_1_ask) / 2
otm_put_2_mid = (otm_put_2_bid + otm_put_2_ask) / 2





#gets option volume 
atm_call_volume = chain.calls[ chain.calls["strike"] == closest_atm_call ]["volume"].iloc[0]
atm_put_volume = chain.puts[ chain.puts["strike"] == closest_atm_put ]["volume"].iloc[0]



otm_call_1_volume = chain.calls[ chain.calls["strike"] == otm_call_1_closest ]["volume"].iloc[0]
otm_call_2_volume = chain.calls[ chain.calls["strike"] == otm_call_2_closest ]["volume"].iloc[0]

otm_put_1_volume  = chain.puts[ chain.puts["strike"] == otm_put_1_closest ]["volume"].iloc[0]
otm_put_2_volume  = chain.puts[ chain.puts["strike"] == otm_put_2_closest ]["volume"].iloc[0]


#implied volatility
atm_call_iv = chain.calls[ chain.calls["strike"] == closest_atm_call ]["impliedVolatility"].iloc[0]
atm_put_iv  = chain.puts[ chain.puts["strike"] == closest_atm_put ]["impliedVolatility"].iloc[0]

otm_call_1_iv = chain.calls[ chain.calls["strike"] == otm_call_1_closest ]["impliedVolatility"].iloc[0]
otm_call_2_iv = chain.calls[ chain.calls["strike"] == otm_call_2_closest ]["impliedVolatility"].iloc[0]

otm_put_1_iv = chain.puts[ chain.puts["strike"] == otm_put_1_closest ]["impliedVolatility"].iloc[0]
otm_put_2_iv = chain.puts[ chain.puts["strike"] == otm_put_2_closest ]["impliedVolatility"].iloc[0]

#open interest

atm_call_oi = chain.calls[ chain.calls["strike"] == closest_atm_call ]["openInterest"].iloc[0]
atm_put_oi  = chain.puts[ chain.puts["strike"] == closest_atm_put ]["openInterest"].iloc[0]

otm_call_1_oi = chain.calls[ chain.calls["strike"] == otm_call_1_closest ]["openInterest"].iloc[0]
otm_call_2_oi = chain.calls[ chain.calls["strike"] == otm_call_2_closest ]["openInterest"].iloc[0]

otm_put_1_oi = chain.puts[ chain.puts["strike"] == otm_put_1_closest ]["openInterest"].iloc[0]
otm_put_2_oi = chain.puts[ chain.puts["strike"] == otm_put_2_closest ]["openInterest"].iloc[0]


#spread

atm_call_spread = atm_call_ask - atm_call_bid
atm_put_spread  = atm_put_ask - atm_put_bid

otm_call_1_spread = otm_call_1_ask - otm_call_1_bid
otm_call_2_spread = otm_call_2_ask - otm_call_2_bid

otm_put_1_spread = otm_put_1_ask - otm_put_1_bid
otm_put_2_spread = otm_put_2_ask - otm_put_2_bid

#spread percent

atm_call_spread_pct = (atm_call_spread / atm_call_mid) * 100
atm_put_spread_pct  = (atm_put_spread  / atm_put_mid)  * 100

otm_call_1_spread_pct = (otm_call_1_spread / otm_call_1_mid) * 100
otm_call_2_spread_pct = (otm_call_2_spread / otm_call_2_mid) * 100

otm_put_1_spread_pct = (otm_put_1_spread / otm_put_1_mid) * 100
otm_put_2_spread_pct = (otm_put_2_spread / otm_put_2_mid) * 100



#timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
#symbol
symbol = stock.ticker

#expiration date
expiration = exp

#snapshot id
snapshot_id = f"{symbol}_{timestamp}"
exp_date = datetime.datetime.strptime(exp, "%Y-%m-%d").date()
days_till_expiry = (exp_date - today).days

if days_till_expiry <= 1:
    time_decay_bucket = "EXTREME"
elif days_till_expiry <= 3:
    time_decay_bucket = "HIGH"
elif days_till_expiry <= 7:
    time_decay_bucket = "MEDIUM"
else:
    time_decay_bucket = "LOW"





#connecting to database

con = duckdb.connect("options_data.db")


con.execute("""
CREATE TABLE IF NOT EXISTS option_snapshots_5w (
    snapshot_id TEXT,
    timestamp TIMESTAMP,
    symbol TEXT,
    strike DOUBLE,
    call_put TEXT,
    days_to_expiry INTEGER,
    moneyness_bucket TEXT,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    volume INTEGER,
    open_interest INTEGER,
    iv DOUBLE,
    spread DOUBLE,
    spread_pct DOUBLE,
    time_decay_bucket TEXT
);
""")

# keep only recent 5 weeks (~35 days)
con.execute("""
DELETE FROM option_snapshots_5w
WHERE timestamp < NOW() - INTERVAL '35 days';
""")



con.execute("""
CREATE TABLE IF NOT EXISTS option_snapshots_5w (
    snapshot_id TEXT,
    timestamp TIMESTAMP,
    symbol TEXT,
    strike DOUBLE,
    call_put TEXT,
    days_to_expiry INTEGER,
    moneyness_bucket TEXT,
    bid DOUBLE,
    ask DOUBLE,
    mid DOUBLE,
    volume INTEGER,
    open_interest INTEGER,
    iv DOUBLE,
    spread DOUBLE,
    spread_pct DOUBLE,
    time_decay_bucket TEXT
);
""")

# keep only recent 5 weeks (~35 days)
con.execute("""
DELETE FROM option_snapshots_5w
WHERE timestamp < NOW() - INTERVAL '35 days';
""")

con.execute("""
INSERT INTO option_snapshots_5w (
    snapshot_id,
    timestamp,
    symbol,
    strike,
    call_put,
    days_to_expiry,
    moneyness_bucket,
    bid,
    ask,
    mid,
    volume,
    open_interest,
    iv,
    spread,
    spread_pct,
    time_decay_bucket
)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""", [py(x) for x in [
    # ATM CALL
    snapshot_id, timestamp, symbol, closest_atm_call, "C", days_till_expiry, "ATM",
    atm_call_bid, atm_call_ask, atm_call_mid, atm_call_volume, atm_call_oi,
    atm_call_iv, atm_call_spread, atm_call_spread_pct, time_decay_bucket,

    # ATM PUT
    snapshot_id, timestamp, symbol, closest_atm_put, "P", days_till_expiry, "ATM",
    atm_put_bid, atm_put_ask, atm_put_mid, atm_put_volume, atm_put_oi,
    atm_put_iv, atm_put_spread, atm_put_spread_pct, time_decay_bucket,

    # OTM CALL 1
    snapshot_id, timestamp, symbol, otm_call_1_closest, "C", days_till_expiry, "OTM_1",
    otm_call_1_bid, otm_call_1_ask, otm_call_1_mid, otm_call_1_volume, otm_call_1_oi,
    otm_call_1_iv, otm_call_1_spread, otm_call_1_spread_pct, time_decay_bucket,

    # OTM PUT 1
    snapshot_id, timestamp, symbol, otm_put_1_closest, "P", days_till_expiry, "OTM_1",
    otm_put_1_bid, otm_put_1_ask, otm_put_1_mid, otm_put_1_volume, otm_put_1_oi,
    otm_put_1_iv, otm_put_1_spread, otm_put_1_spread_pct, time_decay_bucket,

    # OTM CALL 2
    snapshot_id, timestamp, symbol, otm_call_2_closest, "C", days_till_expiry, "OTM_2",
    otm_call_2_bid, otm_call_2_ask, otm_call_2_mid, otm_call_2_volume, otm_call_2_oi,
    otm_call_2_iv, otm_call_2_spread, otm_call_2_spread_pct, time_decay_bucket,

    # OTM PUT 2
    snapshot_id, timestamp, symbol, otm_put_2_closest, "P", days_till_expiry, "OTM_2",
    otm_put_2_bid, otm_put_2_ask, otm_put_2_mid, otm_put_2_volume, otm_put_2_oi,
    otm_put_2_iv, otm_put_2_spread, otm_put_2_spread_pct, time_decay_bucket
]])





# getting all mid option premiums for z-score calculation price
# ATM CALLS
atm_call_df = con.execute(f"""
    SELECT *
    FROM option_snapshots_5w
    WHERE moneyness_bucket = 'ATM'
      AND call_put = 'C'
      AND time_decay_bucket = '{time_decay_bucket}'
""").df()

atm_call_mean = atm_call_df["mid"].mean()
atm_call_std = atm_call_df["mid"].std()
atm_call_z = (atm_call_mid - atm_call_mean) / atm_call_std



# ATM PUTS
atm_put_df = con.execute(f"""
    SELECT *
    FROM option_snapshots_5w
    WHERE moneyness_bucket = 'ATM'
      AND call_put = 'P'
      AND time_decay_bucket = '{time_decay_bucket}'
""").df()

atm_put_mean = atm_put_df["mid"].mean()
atm_put_std = atm_put_df["mid"].std()
atm_put_z = (atm_put_mid - atm_put_mean) / atm_put_std




# OTM_1 CALLS
otm1_call_df = con.execute(f"""
    SELECT *
    FROM option_snapshots_5w
    WHERE moneyness_bucket = 'OTM_1'
      AND call_put = 'C'
      AND time_decay_bucket = '{time_decay_bucket}'
""").df()

otm1_call_mean = otm1_call_df["mid"].mean()
otm1_call_std = otm1_call_df["mid"].std()
otm_call_1_z = (otm_call_1_mid - otm1_call_mean) / otm1_call_std




# OTM_1 PUTS
otm1_put_df = con.execute(f"""
    SELECT *
    FROM option_snapshots_5w
    WHERE moneyness_bucket = 'OTM_1'
      AND call_put = 'P'
      AND time_decay_bucket = '{time_decay_bucket}'
""").df()

otm1_put_mean = otm1_put_df["mid"].mean()
otm1_put_std = otm1_put_df["mid"].std()
otm_put_1_z = (otm_put_1_mid - otm1_put_mean) / otm1_put_std




# OTM_2 CALLS
otm2_call_df = con.execute(f"""
    SELECT *
    FROM option_snapshots_5w
    WHERE moneyness_bucket = 'OTM_2'
      AND call_put = 'C'
      AND time_decay_bucket = '{time_decay_bucket}'
""").df()

otm2_call_mean = otm2_call_df["mid"].mean()
otm2_call_std = otm2_call_df["mid"].std()
otm_call_2_z = (otm_call_2_mid - otm2_call_mean) / otm2_call_std



# OTM_2 PUTS
otm2_put_df = con.execute(f"""
    SELECT *
    FROM option_snapshots_5w
    WHERE moneyness_bucket = 'OTM_2'
      AND call_put = 'P'
      AND time_decay_bucket = '{time_decay_bucket}'
""").df()

otm2_put_mean = otm2_put_df["mid"].mean()
otm2_put_std = otm2_put_df["mid"].std()
otm_put_2_z = (otm_put_2_mid - otm2_put_mean) / otm2_put_std






#volume z-score

#at the money call
atm_call_vol_mean = atm_call_df["volume"].mean()
atm_call_vol_std  = atm_call_df["volume"].std()
atm_call_vol_z    = (atm_call_volume - atm_call_vol_mean) / atm_call_vol_std


#at the money put
atm_put_vol_mean = atm_put_df["volume"].mean()
atm_put_vol_std  = atm_put_df["volume"].std()
atm_put_vol_z    = (atm_put_volume - atm_put_vol_mean) / atm_put_vol_std

#out money 1 call
otm1_call_vol_mean = otm1_call_df["volume"].mean()
otm1_call_vol_std  = otm1_call_df["volume"].std()
otm_call_1_vol_z   = (otm_call_1_volume - otm1_call_vol_mean) / otm1_call_vol_std




#out money 1 put 
otm1_put_vol_mean = otm1_put_df["volume"].mean()
otm1_put_vol_std  = otm1_put_df["volume"].std()
otm_put_1_vol_z   = (otm_put_1_volume - otm1_put_vol_mean) / otm1_put_vol_std

#out money call 2
otm2_call_vol_mean = otm2_call_df["volume"].mean()
otm2_call_vol_std  = otm2_call_df["volume"].std()
otm_call_2_vol_z   = (otm_call_2_volume - otm2_call_vol_mean) / otm2_call_vol_std

#out money put 2
otm2_put_vol_mean = otm2_put_df["volume"].mean()
otm2_put_vol_std  = otm2_put_df["volume"].std()
otm_put_2_vol_z   = (otm_put_2_volume - otm2_put_vol_mean) / otm2_put_vol_std


#implied volatiily iv z-score

# at money call
atm_call_iv_mean = atm_call_df["iv"].mean()
atm_call_iv_std  = atm_call_df["iv"].std()
atm_call_iv_z    = (atm_call_iv - atm_call_iv_mean) / atm_call_iv_std


# at money put
atm_put_iv_mean = atm_put_df["iv"].mean()
atm_put_iv_std  = atm_put_df["iv"].std()
atm_put_iv_z    = (atm_put_iv - atm_put_iv_mean) / atm_put_iv_std

# out money 1 call
otm1_call_iv_mean = otm1_call_df["iv"].mean()
otm1_call_iv_std  = otm1_call_df["iv"].std()
otm_call_1_iv_z   = (otm_call_1_iv - otm1_call_iv_mean) / otm1_call_iv_std


#out money put 1 
otm1_put_iv_mean = otm1_put_df["iv"].mean()
otm1_put_iv_std  = otm1_put_df["iv"].std()
otm_put_1_iv_z   = (otm_put_1_iv - otm1_put_iv_mean) / otm1_put_iv_std

# out money call 2
otm2_call_iv_mean = otm2_call_df["iv"].mean()
otm2_call_iv_std  = otm2_call_df["iv"].std()
otm_call_2_iv_z   = (otm_call_2_iv - otm2_call_iv_mean) / otm2_call_iv_std

# out money put 2 
otm2_put_iv_mean = otm2_put_df["iv"].mean()
otm2_put_iv_std  = otm2_put_df["iv"].std()
otm_put_2_iv_z   = (otm_put_2_iv - otm2_put_iv_mean) / otm2_put_iv_std


# ATM CALL triple signal
if (atm_call_z > 2) and (atm_call_vol_z > 1.5) and (atm_call_iv_z > 1.5):
    message = (
        "ATM CALL TRIPLE SIGNAL FIRED!\n"
        "ACTION: BUY 🔥"
    )
    send_text(message)

# ATM PUT triple signal
if (atm_put_z > 2) and (atm_put_vol_z > 1.5) and (atm_put_iv_z > 1.5):
    message = (
        "ATM PUT TRIPLE SIGNAL FIRED!\n"
        "ACTION: BUY 🔥"
    )
    send_text(message)

# OTM_1 CALL triple signal
if (otm_call_1_z > 2) and (otm_call_1_vol_z > 1.5) and (otm_call_1_iv_z > 1.5):
    message = (
        "OTM_1 CALL TRIPLE SIGNAL FIRED!\n"
        "ACTION: BUY 🔥"
    )
    send_text(message)

# OTM_1 PUT triple signal
if (otm_put_1_z > 2) and (otm_put_1_vol_z > 1.5) and (otm_put_1_iv_z > 1.5):
    message = (
        "OTM_1 PUT TRIPLE SIGNAL FIRED!\n"
        "ACTION: BUY 🔥"
    )
    send_text(message)

# OTM_2 CALL triple signal
if (otm_call_2_z > 2) and (otm_call_2_vol_z > 1.5) and (otm_call_2_iv_z > 1.5):
    message = (
        "OTM_2 CALL TRIPLE SIGNAL FIRED!\n"
        "ACTION: BUY 🔥"
    )
    send_text(message)

# OTM_2 PUT triple signal
if (otm_put_2_z > 2) and (otm_put_2_vol_z > 1.5) and (otm_put_2_iv_z > 1.5):
    message = (
        "OTM_2 PUT TRIPLE SIGNAL FIRED!\n"
        "ACTION: BUY 🔥"
    )
    send_text(message)



con.execute("""
CREATE TABLE IF NOT EXISTS option_snapshots_enriched_5w (
    snapshot_id TEXT,
    timestamp TIMESTAMP,
    symbol TEXT,
    strike DOUBLE,
    call_put TEXT,
    days_to_expiry INTEGER,
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
    iv_z DOUBLE
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
    strike,
    call_put,
    days_to_expiry,
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
    iv_z
)
VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?),
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
""", [py(x) for x in [
    # ATM CALL
    snapshot_id, timestamp, symbol, closest_atm_call, "C", days_till_expiry, "ATM",
    atm_call_bid, atm_call_ask, atm_call_mid, atm_call_volume, atm_call_oi,
    atm_call_iv, atm_call_spread, atm_call_spread_pct, time_decay_bucket,
    atm_call_z, atm_call_vol_z, atm_call_iv_z,

    # ATM PUT
    snapshot_id, timestamp, symbol, closest_atm_put, "P", days_till_expiry, "ATM",
    atm_put_bid, atm_put_ask, atm_put_mid, atm_put_volume, atm_put_oi,
    atm_put_iv, atm_put_spread, atm_put_spread_pct, time_decay_bucket,
    atm_put_z, atm_put_vol_z, atm_put_iv_z,

    # OTM CALL 1
    snapshot_id, timestamp, symbol, otm_call_1_closest, "C", days_till_expiry, "OTM_1",
    otm_call_1_bid, otm_call_1_ask, otm_call_1_mid, otm_call_1_volume, otm_call_1_oi,
    otm_call_1_iv, otm_call_1_spread, otm_call_1_spread_pct, time_decay_bucket,
    otm_call_1_z, otm_call_1_vol_z, otm_call_1_iv_z,

    # OTM PUT 1
    snapshot_id, timestamp, symbol, otm_put_1_closest, "P", days_till_expiry, "OTM_1",
    otm_put_1_bid, otm_put_1_ask, otm_put_1_mid, otm_put_1_volume, otm_put_1_oi,
    otm_put_1_iv, otm_put_1_spread, otm_put_1_spread_pct, time_decay_bucket,
    otm_put_1_z, otm_put_1_vol_z, otm_put_1_iv_z,

    # OTM CALL 2
    snapshot_id, timestamp, symbol, otm_call_2_closest, "C", days_till_expiry, "OTM_2",
    otm_call_2_bid, otm_call_2_ask, otm_call_2_mid, otm_call_2_volume, otm_call_2_oi,
    otm_call_2_iv, otm_call_2_spread, otm_call_2_spread_pct, time_decay_bucket,
    otm_call_2_z, otm_call_2_vol_z, otm_call_2_iv_z,

    # OTM PUT 2
    snapshot_id, timestamp, symbol, otm_put_2_closest, "P", days_till_expiry, "OTM_2",
    otm_put_2_bid, otm_put_2_ask, otm_put_2_mid, otm_put_2_volume, otm_put_2_oi,
    otm_put_2_iv, otm_put_2_spread, otm_put_2_spread_pct, time_decay_bucket,
    otm_put_2_z, otm_put_2_vol_z, otm_put_2_iv_z
]])





con.close()
