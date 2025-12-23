import duckdb
import pandas as pd
import os

def main():
    con = duckdb.connect("options_data.db", read_only=True)

    print("\n=== DB LOCATION CHECK ===")
    print("cwd:", os.getcwd())
    print("db_path:", os.path.abspath("options_data.db"))

    print("\n=== TABLES IN DB ===")
    print(con.execute("SHOW TABLES;").fetchall())

    # ---- SCHEMA CHECK: option_snapshots_enriched ----
    print("\n=== SCHEMA: option_snapshots_enriched ===")
    schema = con.execute("PRAGMA table_info('option_snapshots_enriched');").fetchall()
    for row in schema:
        print(row)

    cols = [r[1] for r in schema]
    required = ["opt_ret_10m", "opt_ret_1h", "opt_ret_eod", "opt_ret_next_open", "opt_ret_1d", "opt_ret_exp"]
    missing = [c for c in required if c not in cols]

    print("\n=== RETURN COLUMNS PRESENT? ===")
    if missing:
        print("MISSING:", missing)
    else:
        print("All return columns present ✅")

    # ---- LATEST ROWS ----
    print("\n=== LATEST ROWS (option_snapshots_enriched) ===")
    df = con.execute("""
        SELECT
            snapshot_id,
            timestamp,
            symbol,
            option_symbol,
            strike,
            call_put,
            days_to_expiry,
            expiration_date,
            moneyness_bucket,
            time_decay_bucket,
            mid,
            volume,
            iv,
            mid_z,
            volume_z,
            iv_z,
            opt_ret_10m,
            opt_ret_1h,
            opt_ret_eod,
            opt_ret_next_open,
            opt_ret_1d,
            opt_ret_exp
        FROM option_snapshots_enriched
        ORDER BY timestamp DESC
        LIMIT 30;
    """).df()

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 160)
    print(df)

if __name__ == "__main__":
    main()
