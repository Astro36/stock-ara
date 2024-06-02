import pandas as pd
import redis
from stock_ainalyst import db


def calculate_betas(market, market_asset_id):
    market_prices = db.find_weekly_close_prices_by_id(market_asset_id)
    asset_prices = db.find_weekly_close_prices_by_exchange(market)
    prices = pd.concat([market_prices, asset_prices], axis=1)
    returns = prices.pct_change(fill_method=None).dropna(how="all")
    covs = returns.cov().fillna(0) * 52
    betas = (covs.loc[market_asset_id] / covs.loc[market_asset_id, market_asset_id]).drop(market_asset_id)
    return betas


def apply_capm(market, market_asset_id, market_expected_return, risk_free_rate):
    betas = calculate_betas(market, market_asset_id)
    return betas * (market_expected_return - risk_free_rate) + risk_free_rate


def apply_reverse_optimization(market):
    market_caps = db.find_market_caps_by_exchange(market)
    market_weights = market_caps / sum(market_caps)
    prices = db.find_weekly_close_prices_by_exchange(market)
    returns = prices.pct_change(fill_method=None).dropna(how="all")
    covs = returns.cov().fillna(0) * 52
    return covs @ market_weights


r = redis.Redis(host="localhost", port=6379, db=0, protocol=3)

risk_free_rate = 0.03567
market_expected_return = 0.0532

### KOSPI ###
market = "KOSPI"
market_asset_id = 1

capm_expected_returns = apply_capm(market, market_asset_id, market_expected_return, risk_free_rate)

pipe = r.pipeline()
for asset_id, expected_return in capm_expected_returns.items():
    r.set(f"capm_expected_return:asset_id#{asset_id}", expected_return)
pipe.execute()

implied_expected_returns = apply_reverse_optimization(market)
risk_aversion = sum(capm_expected_returns * implied_expected_returns) / sum(implied_expected_returns**2)
implied_expected_returns = risk_aversion * implied_expected_returns

pipe = r.pipeline()
for asset_id, expected_return in implied_expected_returns.items():
    r.set(f"implied_expected_return:asset_id#{asset_id}", expected_return)
pipe.execute()

### KOSDAQ ###
market = "KOSDAQ"
market_asset_id = 2

capm_expected_returns = apply_capm(market, market_asset_id, market_expected_return, risk_free_rate)

pipe = r.pipeline()
for asset_id, expected_return in capm_expected_returns.items():
    r.set(f"capm_expected_return:asset_id#{asset_id}", expected_return)
pipe.execute()

implied_expected_returns = apply_reverse_optimization(market)
risk_aversion = sum(capm_expected_returns * implied_expected_returns) / sum(implied_expected_returns**2)
implied_expected_returns = risk_aversion * implied_expected_returns

pipe = r.pipeline()
for asset_id, expected_return in implied_expected_returns.items():
    r.set(f"implied_expected_return:asset_id#{asset_id}", expected_return)
pipe.execute()
