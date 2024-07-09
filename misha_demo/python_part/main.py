import yfinance as yf
import pandas as pd
from internals.minio_helper import MinioDumbHelper

from internals.config_helper import get_config_part
from internals.config_entries import *


def _get_daily_close(tickers: list[str], start_date: str, end_date: str):
    data = yf.download(tickers, start=start_date, end=end_date, interval="1d")
    return data


def get_data_2_parquet(tickers: list[str], year: int, file_name: str) -> None:
    start_date = f'{year}-01-01'
    end_date = f'{year}-12-31'
    data = _get_daily_close(tickers, start_date, end_date)
    data.to_parquet(file_name, engine='pyarrow')


def upload_2_minio(object_name: str,  file_path: str) -> None:
    mdh = MinioDumbHelper()
    mdh.upload(file_name=file_path, object_name=object_name)


def download_from_minio(object_name: str,  file_path: str) -> None:
    mdh = MinioDumbHelper()
    mdh.download(file_name=file_path, object_name=object_name)


def test_upload(object_name: str, year: int):
    tickers = get_config_part(TICKERS)
    file_name = '/tmp/ztickers_up'

    get_data_2_parquet(tickers=tickers, year=year, file_name=file_name)
    upload_2_minio(object_name=object_name, file_path=file_name)


def test_download(object_name: str, file_name: str) -> None:
    download_from_minio(object_name=object_name, file_path=file_name)
    data = pd.read_parquet(file_name, engine='pyarrow')
    a = 2


if __name__ == '__main__':
    test_upload(object_name='/top/misha_data/pd_from_yf', year=2023)
    test_download(object_name='/top/misha_data/pd_from_yf', file_name='/tmp/ztickers_down')
