from tenmin_database import ingest_option_snapshot_3d
import pandas as pd
import time


def get_sp500_symbols():
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    df = pd.read_csv(url)
    return df["Symbol"].tolist()


def main():
    symbols = get_sp500_symbols()

    for symbol in symbols:
        try:
            ingest_option_snapshot_3d(symbol)
        except Exception as e:
            print(f"Error ingesting {symbol}: {e}")

        time.sleep(0.3)  # <-- ALWAYS sleep between symbols


if __name__ == "__main__":
    main()

