import asyncio

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity

from pandas.api.types import is_numeric_dtype

from internals.config_entries import *
from internals.config_helper import get_config_part
from internals.minio_helper import MinioDumbHelper


@activity.defn(name="load_universe")
async def zuniverse(source: str) -> list[str]:
    """

    :param source:
    :return:
    """
    print(f'my load universe called with {source}')
    return ['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'JPM', 'WMT', 'UNH', 'BAC', 'HD']


@activity.defn(name="get_daily_close")
async def get_daily_close2(tickers: list[str], start_date: str, end_date: str) -> str:
    """
    Loading closing prices (from yahoo here), needs to be mocked or used elsewhere

    :param tickers: list(str)
    :param start_date: str
    :param end_date: str
    :return: xarray.DataArray
    """
    print(f'get_daily_close2 input: tickers {tickers}, start date {start_date}, end date {end_date}')
    print(f'in get_daily_close: activity info part workflow_id {activity.info().workflow_id}')
    # Download stock data
    mdh = MinioDumbHelper()

    my_config = get_config_part(GET_DAILY_CLOSE_WORKER_INFO)

    data = mdh.read_pandas(my_config)

    # Check for missing data and raise an error if significant
    missing_data_pct = (data.isnull().sum() / data.size) * 100
    if missing_data_pct.max() > 10:
        raise ValueError(f"Too much missing data (>10%). Consider adjusting data source or handling missing values.")

    # Convert data to xarray.DataArray with columns as tickers and rows as dates
    daily_data = data['Close'].to_xarray().to_dataarray().rename({'variable': 'ticker'})

    # Ensure data types are numeric
    daily_data = daily_data.where(is_numeric_dtype(daily_data))
    print("loaded daily data, dim", daily_data.sizes)

    # write xarray to
    output_minio_object = mdh.write_xarray(x_array=daily_data, conf=my_config)
    return output_minio_object


@activity.defn(name="compute_daily_returns")
async def compute_daily_returns2(daily_data_object_in_minio: str) -> str:
    """
    Compute daily returns from time series of closing prices
    :param daily_data_object_in_minio: minio path to xarray.DataArray converted to cdf4
    :return: minio path to xarray.DataArray converted to cdf4
    """

    mdh = MinioDumbHelper()

    my_config = get_config_part(COMPUTE_DAILY_RETURNS_WORKER_INFO)
    my_config['input_minio_object'] = daily_data_object_in_minio

    daily_data = mdh.read_xarray(my_config)

    dret = daily_data.diff('Date') / daily_data.shift(Date=1)
    print("compute_daily returns, dim", dret.sizes)

    output_minio_object = mdh.write_xarray(x_array=dret, conf=my_config)
    return output_minio_object


@activity.defn(name="z_map_test")
async def zmaptest(ul: list[str]) -> dict:
    for e in ul:
        print(f'my load universe called with {e}')

    return {'key1': True, 'key2': 123}


async def main() -> None:
    my_config = get_config_part(TEMPORAL_INFO)
    target_host = my_config['target_host']
    task_queue = my_config['task_queue']
    client: Client = await Client.connect(target_host=target_host, namespace="default")
    # Run the worker
    worker: Worker = Worker(
        client,
        task_queue=task_queue,
        activities=[zuniverse, zmaptest, get_daily_close2, compute_daily_returns2],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
