import duckdb
import pandas as pd
con = duckdb.connect("options_data.db", read_only=True)

df = con.execute("""
    SELECT *
    FROM option_snapshots_enriched
    ORDER BY timestamp DESC
    LIMIT 6
""").df()

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
print(df)