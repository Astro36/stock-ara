import math
import pandas as pd
from stock_ara.infra.database import asset_price_repository, cache_repository


class StockExpectedReturnManager:
    def update(self, risk_free_rate=0.0360, market_expected_excess_return=0.0532):
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
        for asset_id, expected_return in kospi_implied_expected_returns.items():
            cache_repository.set_implied_expected_return(asset_id, expected_return)

        kospi_capm_expected_returns = apply_capm("KOSPI", 1, risk_free_rate, market_expected_excess_return)
        for asset_id, expected_return in kospi_capm_expected_returns.items():
            cache_repository.set_capm_expected_return(asset_id, expected_return)

        kospi_sharpe_ratio = market_expected_excess_return / math.sqrt(kospi_market_variance)

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
        for asset_id, expected_return in kosdaq_implied_expected_returns.items():
            cache_repository.set_implied_expected_return(asset_id, expected_return)
            
        kosdaq_capm_expected_returns = apply_capm("KOSDAQ", 1, risk_free_rate, kosdaq_market_expected_excess_return)
        for asset_id, expected_return in kosdaq_capm_expected_returns.items():
            cache_repository.set_capm_expected_return(asset_id, expected_return)


def calculate_betas(market, market_asset_id):
    market_prices = asset_price_repository.get_asset_prices(market_asset_id)
    asset_prices = asset_price_repository.get_all_stock_prices(market)
    prices = pd.concat([market_prices, asset_prices], axis=1)
    returns = prices.pct_change(fill_method=None).dropna(how="all")
    covs = returns.cov().fillna(0) * 52
    betas = (covs.loc[market_asset_id] / covs.loc[market_asset_id, market_asset_id]).drop(market_asset_id)
    return betas


def apply_capm(market, market_asset_id, risk_free_rate, market_expected_excess_return):
    betas = calculate_betas(market, market_asset_id)
    return betas * market_expected_excess_return + risk_free_rate
