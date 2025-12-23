import duckdb
import pandas as pd

con = duckdb.connect("options_data.db", read_only=True)

df = con.execute("""
SELECT *
FROM option_snapshots
ORDER BY timestamp DESC
LIMIT 20;
""").df()

print(df)












