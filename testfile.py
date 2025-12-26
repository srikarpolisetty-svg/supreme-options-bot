import duckdb

con = duckdb.connect("options_data.db", read_only=True)



con.execute("""
    SELECT *
    FROM option_snapshots_enriched
    ORDER BY timestamp DESC
    LIMIT 6;
""").fetchall()
