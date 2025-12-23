import duckdb
import pandas as pd
import os

def main():
    # Safety: read-only connection
    con = duckdb.connect("options_data.db", read_only=True)

    print("\n=== DB LOCATION CHECK ===")
    print("cwd:", os.getcwd())
    print("db_path:", os.path.abspath("options_data.db"))

    print("\n=== TABLES IN DB ===")
    print(con.execute("SHOW TABLES;").fetchall())

    print("\n=== SCHEMA: option_snapshots ===")
    schema = con.execute("""
        PRAGMA table_info('option_snapshots');
    """).fetchall()
    for row in schema:
        print(row)

    print("\n=== LATEST ROWS ===")
    df = con.execute("""
        SELECT *
        FROM option_snapshots
        ORDER BY timestamp DESC
        LIMIT 20;
    """).df()

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 140)
    print(df)

if __name__ == "__main__":
    main()











