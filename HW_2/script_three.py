import polars as pl
import glob
import argparse


def load_files(num_files, path_or_dir_files):
    files = glob.glob(f"{path_or_dir_files}/*.parquet")

    if num_files == "one":
        lf = pl.read_parquet(path_or_dir_files)
    elif num_files == "more":
        lf = pl.concat([pl.read_parquet(f) for f in files])

    return lf


def candle(trades, interval):
    candle = trades.with_columns(
        pl.col("timestamp").dt.truncate(interval).alias("interval"),
        pl.col("price") * pl.col("quantity").alias("quote_qty")

    ).group_by(["interval", 'is_buyer_maker']).agg([
        pl.col("timestamp").first().alias("timestamp"),
        pl.col("price").mean().alias("price"),
        pl.col("quantity").sum().alias("quantity"),
        pl.col("quote_qty").sum().alias("quote_qty"),
        pl.col("price").count().alias("num_trades"),
        pl.col("is_buyer_maker").all().alias("volume")
    ])

    return candle

def setup_arg_parser():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-f', '--file_path',
        required=True,
        type=str,  # автоматически преобразует в верхний регистр
        help='Укажи имя файла или путь к папке с файлами',
        metavar='PATH'
    )

    parser.add_argument(
        '-n', '--num_files',
        required=True,
        type=str,  # автоматически преобразует в верхний регистр
        help='Укажи cколько файлов будет: one or more',
        metavar='num_files'
    )

    parser.add_argument(
        '-i', '--interval',
        required =True,
        type=str,
        default= '1m',
        help='Укажи величину интервала',
        metavar='INT',
    )

    return value

def main():
    parser = setup_arg_parser()
    args = parser.parse_args()

    df = load_files(args.num_files, args.PATH)

    trades = df.with_columns(
        pl.col("timestamp").cast(pl.Datetime)
    ).sort("timestamp")

    candle(trades, args.interval)

if __name__ == "__main__":
    main()