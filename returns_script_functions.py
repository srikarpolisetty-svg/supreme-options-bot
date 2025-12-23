import duckdb


con = duckdb.connect("options_data.db")



def fill_return_label(label_name, time_condition, extra_where=""):
    """
    label_name: column to update, e.g. 'opt_ret_10m'
    time_condition: SQL condition for selecting the future snapshot, e.g.
                    "f.timestamp >= base.timestamp + INTERVAL 10 MINUTE"
    extra_where: optional SQL filter for the UPDATE WHERE clause
    """
    con.execute(f"""
        UPDATE option_snapshots_enriched base
        SET {label_name} = (
            SELECT (f.mid - base.mid) / base.mid
            FROM option_snapshots_enriched f
            WHERE f.symbol           = base.symbol
              AND f.strike           = base.strike
              AND f.call_put         = base.call_put
              AND f.expiration_date  = base.expiration_date
              AND {time_condition}
            ORDER BY f.timestamp
            LIMIT 1
        )
        WHERE {label_name} IS NULL
        {extra_where};
    """)





def fill_return_label_5w(label_name, time_condition, extra_where=""):
    """
    Same logic as fill_return_label, but updates option_snapshots_enriched_5w.
    """
    con.execute(f"""
        UPDATE option_snapshots_enriched_5w base
        SET {label_name} = (
            SELECT (f.mid - base.mid) / base.mid
            FROM option_snapshots_enriched_5w f
            WHERE f.symbol           = base.symbol
              AND f.strike           = base.strike
              AND f.call_put         = base.call_put
              AND f.expiration_date  = base.expiration_date
              AND {time_condition}
            ORDER BY f.timestamp
            LIMIT 1
        )
        WHERE {label_name} IS NULL
        {extra_where};
    """)
