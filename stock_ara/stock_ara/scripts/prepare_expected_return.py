import math
import redis
from stock_ara.infra.database import asset_price_repository


r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True, protocol=3)

risk_free_rate = 0.0360
market_expected_excess_return = 0.0532


# ### KOSPI ###
kospi_market_prices = asset_price_repository.get_asset_prices(1)
kospi_market_returns = kospi_market_prices.pct_change(fill_method=None).dropna(how="all")
kospi_market_variance = kospi_market_returns.var().values[0] * 52

kospi_market_risk_aversion = market_expected_excess_return / kospi_market_variance

kospi_market_caps = asset_price_repository.get_all_stock_market_caps("KOSPI")
kospi_market_weights = kospi_market_caps / sum(kospi_market_caps)
kospi_asset_prices = asset_price_repository.get_all_stock_prices("KOSPI")
kospi_asset_excess_returns = kospi_asset_prices.pct_change(fill_method=None).dropna(how="all") - (risk_free_rate / 52)
kospi_asset_covs = kospi_asset_excess_returns.cov().fillna(0) * 52

kospi_implied_expected_returns = (kospi_market_risk_aversion * kospi_asset_covs @ kospi_market_weights) + risk_free_rate

kospi_sharpe_ratio = market_expected_excess_return / math.sqrt(kospi_market_variance)

pipe = r.pipeline()
for asset_id, expected_return in kospi_implied_expected_returns.items():
    r.set(f"implied_expected_return:asset_id#{asset_id}", expected_return)
pipe.execute()

# ### KOSDAQ ###
kosdaq_market_prices = asset_price_repository.get_asset_prices(2)
kosdaq_market_returns = kosdaq_market_prices.pct_change(fill_method=None).dropna(how="all")
kosdaq_market_variance = kosdaq_market_returns.var().values[0] * 52

kosdaq_market_expected_excess_return = kospi_sharpe_ratio * math.sqrt(kosdaq_market_variance)
kosdaq_market_risk_aversion = kosdaq_market_expected_excess_return / kosdaq_market_variance

kosdaq_market_caps = asset_price_repository.get_all_stock_market_caps("KOSDAQ")
kosdaq_market_weights = kosdaq_market_caps / sum(kosdaq_market_caps)
kosdaq_asset_prices = asset_price_repository.get_all_stock_prices("KOSDAQ")
kosdaq_asset_excess_returns = kosdaq_asset_prices.pct_change(fill_method=None).dropna(how="all") - (risk_free_rate / 52)
kosdaq_asset_covs = kosdaq_asset_excess_returns.cov().fillna(0) * 52

kosdaq_implied_expected_returns = (kosdaq_market_risk_aversion * kosdaq_asset_covs @ kosdaq_market_weights) + risk_free_rate

pipe = r.pipeline()
for asset_id, expected_return in kosdaq_implied_expected_returns.items():
    r.set(f"implied_expected_return:asset_id#{asset_id}", expected_return)
pipe.execute()
