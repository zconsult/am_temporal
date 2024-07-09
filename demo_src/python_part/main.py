import xarray
import yfinance as yf
import pandas as pd
from internals.minio_helper import MinioDumbHelper

from internals.config_helper import get_config_part
from internals.config_entries import *

import matplotlib.pyplot as plt


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


def generate_report(res, fname=None):
    """
    Reporting, attribution, ect
    :param res: xarray.DataArray time series for portfolio and benchmark returns
    :param fname: str file name to save if not None
    :return: None
    """
    foo = res['portfolioReturn'].cumsum()
    boo = res['benchmarkReturn'].cumsum()
    plt.plot(foo.coords['Date'], foo.to_numpy(), color='green', label='portfolio')
    plt.plot(foo.coords['Date'], boo.to_numpy(), color='blue', label='benchmark')
    plt.legend()
    plt.show()
    if fname is not None:
        assert isinstance(fname, object)
        plt.savefig(fname)
    return fname


def test_plot():
    mdh = MinioDumbHelper()

    mdh.download(file_name='/tmp/sim100_for_plot', object_name='/top/backtester/sim_output100')
    o1 = xarray.open_dataset(filename_or_obj='/tmp/sim100_for_plot')
    generate_report(o1)

    mdh.download(file_name='/tmp/sim300_for_plot', object_name='/top/backtester/sim_output300')
    o2 = xarray.open_dataset(filename_or_obj='/tmp/sim300_for_plot')

    generate_report(o2)


if __name__ == '__main__':
    test_plot()
