import asyncio

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import activity

from internals.config_entries import *
from internals.config_helper import get_config_part
from internals.minio_helper import MinioDumbHelper

import xarray as xr
import numpy as np
from math import tanh


@activity.defn(name="load_inv_risk")
async def load_inv_risk(load_inv_risk_object_in_minio: str) -> str:
    """
    Computes inverted covariance matrix based on the time series of returns (a hack)

    :param load_inv_risk_object_in_minio: minio path to xarray.DataArray converted to cdf4
    :return: minio path to xarray.DataArray converted to cdf4
    """
    print("\nload_inv_risk started\n")
    mdh = MinioDumbHelper()

    my_config = get_config_part(LOAD_INV_RISK_WORKER_INFO)
    my_config['input_minio_object'] = load_inv_risk_object_in_minio

    daily_returns = mdh.read_xarray(my_config)

    tickers = daily_returns.indexes['ticker']
    R = np.cov(daily_returns.to_numpy())
    iR = np.linalg.inv(R)
    xiR = xr.DataArray(iR, coords={'ticker': tickers, 't1': tickers})
    print("loaded and inverted covariance, dim", xiR.sizes)

    output_minio_object = mdh.write_xarray(x_array=xiR, conf=my_config)
    print("\nload_inv_risk finished!\n")
    return output_minio_object


@activity.defn(name="compute_alpha")
async def compute_alpha(compute_alpha_object_in_minio: str) -> str:
    """
    :param compute_alpha_object_in_minio: minio path to xarray.DataArray converted to cdf4 -- Daily Returns
    :return: minio path to xarray.DataArray converted to cdf4
    """
    print("\ncompute_alpha started\n")

    mdh = MinioDumbHelper()

    my_config = get_config_part(COMPUTE_ALPHA_WORKER_INFO)
    my_config['input_minio_object'] = compute_alpha_object_in_minio

    drets = mdh.read_xarray(my_config)

    (nd, nt) = (drets.sizes["Date"], drets.sizes["ticker"])
    alpha = drets + np.random.normal(size=[nt, nd])
    print("loaded signals, dim", alpha.sizes)

    output_minio_object = mdh.write_xarray(x_array=alpha, conf=my_config)
    print("\ncompute_alpha finished\n")
    return output_minio_object


@activity.defn(name="load_benchmarks")
async def load_benchmarks(load_benchmarks_object_in_minio: str) -> str:
    """
    loading a time series for benchmark portfolio weight
    Here, approximating with equal weighting

    :param load_benchmarks_object_in_minio: minio path to xarray.DataArray converted to cdf4
    :return: minio path to xarray.DataArray converted to cdf4
    """
    print("\nload_benchmarks started\n")
    mdh = MinioDumbHelper()

    my_config = get_config_part(LOAD_BENCHMARKS_WORKER_INFO)
    my_config['input_minio_object'] = load_benchmarks_object_in_minio

    drets = mdh.read_xarray(my_config)

    nt = drets.sizes["ticker"]
    p0 = xr.DataArray(1 / nt, coords=drets.coords)
    print("loaded benchmarks, dim", p0.sizes)

    output_minio_object = mdh.write_xarray(x_array=p0, conf=my_config)
    print("\nload_benchmarks finished\n")
    return output_minio_object


@activity.defn(name="run_sim")
async def run_sim(sigmoid=100) -> str:
    """
    single simulation rum loops over days.

    :param sigmoid: float a trading parameter to optimize
    :return: xarray.DataArray time series for portfolio and benchmark returns
    """
    print(f'\nrun_sim with sigmoid = {sigmoid} stared\n')
    mdh = MinioDumbHelper()
    my_config = get_config_part(RUN_SIM_WORKER_INFO)
    download_file = '/tmp/run_sim_' + str(sigmoid)

    mdh.download(file_name=download_file, object_name=my_config['input_daily'])
    daily_returns = xr.open_dataarray(download_file)

    mdh.download(file_name=download_file, object_name=my_config['input_inv_risk'])
    xiR = xr.open_dataarray(download_file)

    mdh.download(file_name=download_file, object_name=my_config['input_alpha'])
    alpha = xr.open_dataarray(download_file)

    mdh.download(file_name=download_file, object_name=my_config['input_benchmark'])
    p0 = xr.open_dataarray(download_file)

    dt_coord = daily_returns.coords['Date']
    bmReturn = xr.DataArray(None, dims=['Date'], coords={'Date': dt_coord}, name='benchmarkReturn')
    pfReturn = xr.DataArray(None, dims=['Date'], coords={'Date': dt_coord}, name='portfolioReturn')

    for d in dt_coord:
        dstr = str(d)[:10]
        a = alpha.sel(Date=d)
        # compute optimal positions with positive soft-constraint
        weights = [1 + tanh(x) for x in xr.dot(a, xiR) / sigmoid] * p0.sel(Date=d)
        weights = weights / weights.sum()
        bmReturn.loc[d] = xr.dot(daily_returns.sel(Date=d), weights).to_numpy().max()
        pfReturn.loc[d] = xr.dot(daily_returns.sel(Date=d), p0.sel(Date=d))
    # (pfReturn - bmReturn).mean().to_numpy()
    res = xr.merge([bmReturn, pfReturn], compat="equals")
    print("generated simulation results, dim ", res.sizes)

    upload_file = '/tmp/run_sim_upload_' + str(sigmoid)
    run_sim_minio_object = my_config['output_minio_object_template'] + str(sigmoid)

    res.to_netcdf(path=upload_file, mode='w')
    mdh.upload(file_name=upload_file, object_name=run_sim_minio_object)

    print(f'\nrun_sim with sigmoid = {sigmoid} finished\n')
    return run_sim_minio_object


async def main() -> None:
    my_config = get_config_part(TEMPORAL_INFO)
    target_host = my_config['target_host']
    task_queue = my_config['task_queue']
    client: Client = await Client.connect(target_host=target_host, namespace="default")
    # Run the worker
    worker: Worker = Worker(
        client,
        task_queue=task_queue,
        activities=[load_inv_risk, compute_alpha, load_benchmarks, run_sim],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
