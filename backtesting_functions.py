
import duckdb

def backtest_returns(
    con,
    moneyness: str,
    call_put: str,
    signal_col: str
):
    query = f"""
        SELECT
            timestamp,
            symbol,
            strike,
            call_put,
            moneyness_bucket,

            opt_ret_10m,
            opt_ret_1h,
            opt_ret_eod,
            opt_ret_next_open,
            opt_ret_1d,
            opt_ret_exp
        FROM option_snapshots_enriched
        WHERE moneyness_bucket = '{moneyness}'
          AND call_put = '{call_put}'
          AND {signal_col} = TRUE
    """
    return con.execute(query).df()