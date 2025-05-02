import requests
import zipfile
import os
import io
import pandas as pd
import datetime
from datetime import datetime, timedelta
import threading
import argparse

def get_zip(url: str, path: str, filename: str):
    """
    :param url: Url файла, который надо скачать
    :param path: Путь до директории, в которую надо скачать файл
    :param filename: Имя файла
    """
    with open(os.path.join(path, filename), "wb") as file:
        response = requests.get(url)
        file.write(response.content)


def unpack(path_to_zip: str, outpath: str, currency_pair: str, date):
    """
    :param path_to_zip: Путь до архива
    :param outpath: Путь до директории, в которую надо распаковать файл
    """

    csv_filename_in_zip = f"{currency_pair}-trades-{date}"
    columns = ["trade_id", "price", "quantity", "quote_qty", "timestamp", "is_buyer_maker", "is_best_match"]

    with zipfile.ZipFile(path_to_zip, 'r') as zip_ref:
      with zip_ref.open(f"{csv_filename_in_zip}.csv") as csv_file:
          df = pd.read_csv(io.TextIOWrapper(csv_file, encoding='utf-8'), header = None)
          df.columns = [name for name in columns]
    df.to_parquet(f"{outpath}/{csv_filename_in_zip}.parquet", engine = "pyarrow")


def download_file(url, path_dir: str, outpath: str, file_name):

    get_zip(url, path_dir, file_name)

    currency_pair = url[url.rfind('/')+1: url.rfind("-trades")]

    date = url[url.rfind('trades-') +7: url.rfind(".zip")]

    zip_path_dir = "other_files/" + file_name

    unpack(zip_path_dir, outpath, currency_pair, date)


def collecting_ULR(currency_pair):
  date_now = datetime.now().date()

  date_start = datetime(year=2025, month=1, day=1).date()

  urls = []

  date = date_start

  while date != date_now:

    url = f"https://data.binance.vision/data/spot/daily/trades/{currency_pair}/{currency_pair}-trades-{date}.zip"

    urls.append(url)

    date += timedelta(days=1)

  return urls

def start_threads(urls: list, function, path_dir, outpath):

    threads = []

    for i, url in enumerate(urls):
      thread = threading.Thread(target=function, args=(url, path_dir, outpath, f"file_{i}.zip"))
      threads.append(thread)
      thread.start()

    for thread in threads:
        thread.join()

def download_full_data(currency_pair: str, num_thread: int):


    outpath = currency_pair
    path_dir_other_files = "other_files"

    os.makedirs(outpath, exist_ok = True)
    os.makedirs(path_dir_other_files, exist_ok = True)

    urls = collecting_ULR(currency_pair)

    for i in range(0, len(urls), num_thread):
      start_threads(urls[i:i+num_thread], download_file, path_dir_other_files, outpath)



def setup_arg_parser():
    parser = argparse.ArgumentParser(
        description='Обработчик рыночных данных с настройкой валютной пары и потоков'
    )

    parser.add_argument(
        '-c', '--currency',
        required=True,
        type=str.upper,  # автоматически преобразует в верхний регистр
        help='Котируемая валютная пара (например: BTCUSDT, ETHUSD)',
        metavar='PAIR'
    )

    parser.add_argument(
        '-t', '--threads',
        type=int,
        default=1,
        choices=range(1, 33),  # ограничение от 1 до 32 потоков
        help='Количество потоков для обработки (1-32, по умолчанию: 1)',
        metavar='THREADS'
    )

    return value


def main():
    parser = setup_arg_parser()
    args = parser.parse_args()

    download_full_data(args.currency, args.threads)


if __name__ == "__main__":
    main()