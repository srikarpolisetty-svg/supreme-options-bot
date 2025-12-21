from execution_functions import get_access_token
from config import SECRET_KEY, ACCOUNT_ID
from execution_functions import preflight_single_leg_option
from execution_functions import place_equity_order
from execution_functions import get_order_status
import duckdb
from execution_functions import to_float
from execution_functions import get_portfolio
from execution_functions import get_daily_unrealized_pnl
from datetime import datetime, timezone, timedelta

from execution_functions import place_close_order
from execution_functions import place_stop_close_order

from execution_functions import trail_exit_signals
from datetime import datetime, timezone


token_response = get_access_token(SECRET_KEY)



data = get_portfolio(ACCOUNT_ID, token_response)


options_bp = to_float(data["buyingPower"]["optionsBuyingPower"])

positions = data.get("positions")

if not positions:
    print("No positions to close")

orders = data.get("orders")

PER_TRADE_RISK_PCT = 0.02

PER_DAY_RISK_PCT = 0.04

max_risk_per_trade = options_bp * PER_TRADE_RISK_PCT

max_risk_per_day = options_bp * PER_DAY_RISK_PCT

execute_trades = False







ACTIVE_STATUSES = {
    "NEW",
    "PARTIALLY_FILLED",
}

# puts in a list
active_buy_open_market = [
    o for o in orders
    if o.get("side") == "BUY"
    and o.get("openCloseIndicator") == "OPEN"
    and o.get("type") == "MARKET"
    and o.get("status") in ACTIVE_STATUSES
]





num_buy_open_market = len(active_buy_open_market)

if num_buy_open_market >=5:
    execute_trades = False
else:
    execute_trades = True



now = datetime.now(timezone.utc)

for order in active_buy_open_market:
    created_at_str = order.get("createdAt")
    if not created_at_str:
        continue

    created_at = datetime.fromisoformat(
        created_at_str.replace("Z", "+00:00")
    )

    if (now - created_at).total_seconds() <= 15 * 60:
        execute_trades = False
        continue









daily_unrealized = get_daily_unrealized_pnl(data)

if daily_unrealized < 0 and abs(daily_unrealized) >= max_risk_per_day:
    for pos in positions:
        place_close_order(
            ACCOUNT_ID,
            token_response,
            order_id=pos["orderId"],
            symbol=pos["instrument"]["symbol"],
            side="SELL",          # closing a long
            order_type="MARKET",
            quantity=pos["quantity"]
        )









con = duckdb.connect("options_data.db")


df = con.execute("""
    SELECT *
    FROM options_snapshots_enriched
    ORDER BY timestamp DESC
    LIMIT 6
""").df()


signal_cols = [
    "atm_call_signal",
    "atm_put_signal",
    "otm1_call_signal",
    "otm1_put_signal",
    "otm2_call_signal",
    "otm2_put_signal",
]



for row in df.itertuples(index=False):
    fired_signals = [col for col in signal_cols if getattr(row, col)]

    if not fired_signals:
        continue

    # Preflight risk check
    flight = preflight_single_leg_option(
        account_id=ACCOUNT_ID,
        access_token=token_response,
        symbol=row.option_symbol,
        quantity=1
    )
    if flight.get("estimatedCost", float("inf")) <= max_risk_per_trade:
        place_equity_order(
            account_id=ACCOUNT_ID,
            access_token=token_response,
            symbol=row.option_symbol,
            side="BUY",
            quantity=2,
            execute=False
        )











            

# turn this into a function so variables are local ?
# move stop loss up to gurantee no loss of money

positions = data.get("positions", [])

if positions:
    for pos in data.get("positions", []):
        option_return = to_float(
            (pos.get("costBasis") or {}).get("gainPercentage")
        )

        if option_return >= 25:
            entry_price = to_float(
                (pos.get("costBasis") or {}).get("unitCost")
            )

            stop_price = entry_price

            place_stop_close_order(
                ACCOUNT_ID,
                token_response,
                symbol=pos["instrument"]["symbol"],
                side="SELL",
                quantity=pos["quantity"],
                stop_price=stop_price
            )

  







positions = data.get("positions", [])

if positions:
    for pos in positions:
        option_return = to_float(
            (pos.get("costBasis") or {}).get("gainPercentage")
        )
        if option_return is None:
            continue

        quantity = int(to_float(pos.get("quantity") or 0))

        symbol = (pos.get("instrument") or {}).get("symbol")
        if not symbol:
            continue

        if option_return >= 50 and quantity >= 2:
            place_close_order(
                ACCOUNT_ID,
                token_response,
                symbol,
                quantity=1
            )








trail_exit_signals(data=data,token_response=token_response)
