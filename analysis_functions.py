def get_latest_snapshot(con, table: str, call_put: str, moneyness_bucket: str):
    """
    Grab the latest snapshot row for a given table / call_put / moneyness bucket.
    Returns a 1-row DataFrame (so you can use .iloc[0] like before).
    """
    query = f"""
        SELECT *
        FROM {table}
        WHERE call_put = ?
          AND moneyness_bucket = ?
        ORDER BY snapshot_id DESC
        LIMIT 1
    """
    return con.execute(query, [call_put, moneyness_bucket]).df()




def load_all_groups(con):
    tables = {
        "short": "option_snapshots_enriched",
        "long":  "option_snapshots_enriched_5w"
    }

    buckets = ["ATM", "OTM_1", "OTM_2"]
    sides = ["C", "P"]  # C = Call, P = Put
    
    data = {}

    for bucket in buckets:
        for side in sides:
            key = f"{bucket}_{'CALL' if side=='C' else 'PUT'}"

            data[key] = {
                "short": get_latest_snapshot(con, tables["short"], side, bucket),
                "long":  get_latest_snapshot(con, tables["long"],  side, bucket)
            }

    return data







def get_option_metrics(groups, key: str):
    """
    groups: dict from load_all_groups()
    key: e.g. "ATM_PUT", "ATM_CALL", "OTM_1_CALL", "OTM_2_PUT", etc.
    """
    short_df = groups[key]["short"]
    long_df  = groups[key]["long"]

    short_row = short_df.iloc[0]
    long_row  = long_df.iloc[0]

    return {
        "short": {
            "z_price":     short_row["mid_z"],
            "z_volume":    short_row["volume_z"],
            "z_iv":        short_row["iv_z"],
            "strike":      short_row["strike"],
            "price":       short_row["mid"],
            "symbol":      short_row["symbol"],
            "snapshot_id": short_row["snapshot_id"],
        },
        "long": {
            "z_price":     long_row["mid_z"],
            "z_volume":    long_row["volume_z"],
            "z_iv":        long_row["iv_z"],
            "snapshot_id": long_row["snapshot_id"],
        }
    }








def update_signal(con, short_snapshot_id, long_snapshot_id, call_put, bucket, signal_column):
    # Update short-term table
    con.execute(f"""
        UPDATE option_snapshots_enriched
        SET {signal_column} = TRUE
        WHERE snapshot_id = ?
          AND call_put = ?
          AND moneyness_bucket = ?;
    """, [short_snapshot_id, call_put, bucket])

    # Update long-term table
    con.execute(f"""
        UPDATE option_snapshots_enriched_5w
        SET {signal_column} = TRUE
        WHERE snapshot_id = ?
          AND call_put = ?
          AND moneyness_bucket = ?;
    """, [long_snapshot_id, call_put, bucket])
