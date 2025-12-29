from tenmin_database import ingest_option_snapshot_3d

import pandas as pd

def get_sp500_symbols():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    return df["Symbol"].tolist()

symbols = get_sp500_symbols()


for symbol in symbols:
    ingest_option_snapshot_3d(symbol)


