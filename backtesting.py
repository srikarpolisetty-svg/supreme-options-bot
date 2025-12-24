import duckdb 
from backtesting_functions import backtest_signal
from backtesting_functions import backtest_returns
from backtesting_functions import backtest_returns_5w

con = duckdb.connect("options.duckdb", read_only=True)


Atmcalldf = backtest_signal(
    con,
    moneyness="ATM",
    call_put="C",
    signal_col="atm_call_signal"
)


print(Atmcalldf)



Atmputdf = backtest_signal(
    con,
    moneyness="ATM",
    call_put="P",
    signal_col="atm_put_signal"
)

print(Atmputdf)





Atmput_general_df = backtest_returns(
    con,
    moneyness="ATM",
    call_put="P",
) 

print (Atmput_general_df)


Atmput_general_df_5w = backtest_returns_5w(
    con,
    moneyness="ATM",
    call_put="P",
) 

print(Atmput_general_df_5w)