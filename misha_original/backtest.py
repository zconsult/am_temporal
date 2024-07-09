import pandas as pd
from activities import *
# WORKFLOW
# Define year and list of ticker symbols (replace with your top 10)
year = 2023
start_date = pd.Timestamp(f"{year}-01-01", tz=None)
end_date = pd.Timestamp(f"{year}-12-31", tz=None)

# define simulation parameters
lmbd = 1         # risk aversion parameters
softLimit = 1000 # sigmoid sharpness

# 1. Load universe
tickers = load_universe()

# 2. Load universe characteristics
sectors = load_characteristics(tickers)

# 3. Data load
daily_data = get_daily_close(tickers, start_date, end_date)

# 4. Load market data
daily_returns = compute_daily_returns(daily_data)

# Convenience variables
# (dates, tickers) = (daily_returns.indexes['Date'], daily_returns.indexes['ticker'])
#(nd, nt) = (daily_returns.sizes["Date"], daily_returns.sizes["ticker"])

# 5. Compute/load covariance matrix
xiR = load_invRisk(daily_returns)

# 6. Compute/Load signals or factors
alpha = compute_alpha(daily_returns)

# 7. Compute load benchmarks
p0 = load_benchmarks(daily_returns)

# 8. Run simulation
res300 = run_sim(daily_returns, p0, xiR, alpha, sigmoid=300)
res100 = run_sim(daily_returns, p0, xiR, alpha, sigmoid=100)

# 9. Process results e.g. attribution
generate_report(res300)
generate_report(res100)