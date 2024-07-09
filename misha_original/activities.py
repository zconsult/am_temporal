import yfinance as yf
from pandas.api.types import is_numeric_dtype
import xarray as xr
import numpy as np
from math import tanh
import matplotlib.pyplot as plt


### ACTIVITIES ###

def load_universe():
    return (['AAPL', 'MSFT', 'GOOG', 'AMZN', 'TSLA', 'JPM', 'WMT', 'UNH', 'BAC', 'HD'])


def load_characteristics(tickers):
    return ({'AAPL': "Tech", 'AMZN': "Tech", 'BAC': "Banking", 'GOOG': "Tech", 'HD': "Consumer", 'JPM': "Banking",
             'MSFT': "Tech", 'TSLA': "Tech", 'UNH': "Health", 'WMT': "Consumer"})


def get_daily_close(tickers, start_date, end_date):
    """
    Loading closing prices (from yahoo here), needs to be mocked or used elsewhere
    :param tickers: list(str)
    :param start_date: str
    :param end_date: str
    :return: xarray.DataArray
    """
    # Download stock data using yfinance (mock for GS)
    data = yf.download(tickers, start=start_date, end=end_date, interval="1d")

    # Filter for business days using pandas is_business_day
    ### ? ###  data = data.loc[pd.is_business_day(data.index)]

    # Check for missing data and raise an error if significant
    missing_data_pct = (data.isnull().sum() / data.size) * 100
    if missing_data_pct.max() > 10:
        raise ValueError(f"Too much missing data (>10%). Consider adjusting data source or handling missing values.")

    # Convert data to xarray.DataArray with columns as tickers and rows as dates
    daily_data = data['Close'].to_xarray().to_dataarray().rename({'variable': 'ticker'})

    # Ensure data types are numeric
    daily_data = daily_data.where(is_numeric_dtype(daily_data))
    print("loaded daily data, dim", daily_data.sizes)
    return (daily_data)


# Calculate daily returns (pct change)
def compute_daily_returns(daily_data):
    """
    Compute daily returns from time series of closing prices
    :param daily_data: xarray.DataArray
    :return: xarray.DataArray
    """
    dret = daily_data.diff('Date') / daily_data.shift(Date=1)
    print("loaded daily returns, dim", dret.sizes)
    return (dret)


# get inverted risk covariance matrix
def load_invRisk(daily_returns):
    """
    Computes inverted covariance matrix based on the time series of returns (a hack)
    :param daily_returns: xarray.DataArray
    :return: xarray.DataArray
    """
    tickers = daily_returns.indexes['ticker']
    R = np.cov(daily_returns.to_numpy())
    iR = np.linalg.inv(R)
    xiR = xr.DataArray(iR, coords={'ticker': tickers, 't1': tickers})
    print("loaded and inverted covariance, dim", xiR.sizes)
    return (xiR)


def compute_alpha(drets):
    """
    :param drets: xarray.DataArray -- Daily Returns
    :return: xarray.DataArray conformal array of signals
    """
    (nd, nt) = (drets.sizes["Date"], drets.sizes["ticker"])
    alpha = drets + np.random.normal(size=[nt, nd])
    print("loaded signals, dim", alpha.sizes)
    return (alpha)


def load_benchmarks(drets):
    """
    loading a time seies for benchmark portofolio weight
    Here, approximating with equal weighting
    :param drets:
    :return:
    """
    nt = drets.sizes["ticker"]
    p0 = xr.DataArray(1 / nt, coords=drets.coords)
    print("loaded benchmarks, dim", p0.sizes)
    return p0


def run_sim(daily_returns, p0, xiR, alpha, sigmoid=100):
    """
    single simulation rum loops over days.
    :param daily_returns: xarray.DataArray
    :param p0: xarray.DataArray benchmark weights same shape as daily_returns
    :param xiR: xarray.DataArray inverted covariance matrix
    :param alpha: xarray.DataArray combined signal, same shape and daily_returns
    :param sigmoid: float a trading parameter to optimize
    :return: xarray.DataArray time series for portfolio and benchmark returns
    """
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
    return res


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
    