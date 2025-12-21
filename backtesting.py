import duckdb 
from backtesting_functions import backtest_returns

con = duckdb.connect("options.duckdb")  # change if needed

Atmcalldf = backtest_returns(
    con,
    moneyness="ATM",
    call_put="C",
    signal_col="atm_call_signal"
)


print(Atmcalldf)



Atmputdf = backtest_returns(
    con,
    moneyness="ATM",
    call_put="P",
    signal_col="atm_put_signal"
)

print(Atmputdf)