from tenmin_database import ingest_option_snapshot_3d
import pandas as pd
import time
import argparse
from tenmin_database import master_ingest  # <-- match your 5W pattern (create this if you don't have it)


def get_sp500_symbols():
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    df = pd.read_csv(url)
    return df["Symbol"].tolist()


def main():
    # ---- shard args ----
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    args = parser.parse_args()

    # ---- load + shard symbols ----
    symbols = get_sp500_symbols()
    symbols = sorted(symbols)  # stable ordering

    my_symbols = symbols[args.shard::args.shards]

    print(f"[3D] Shard {args.shard}/{args.shards} processing {len(my_symbols)} symbols")

    # ---- process ----
    results = []

    for symbol in my_symbols:
        try:
            res = ingest_option_snapshot_3d(symbol, args.shard)  # pass shard if your function supports it

            if not res:
                print(f"Skip {symbol}: no data/expirations")
            else:
                results.append(res)

        except Exception as e:
            print(f"Error ingesting {symbol}: {e}")

        time.sleep(0.3)

    master_ingest(results)  # ---- master ingest (RUN ONCE)


if __name__ == "__main__":
    main()
