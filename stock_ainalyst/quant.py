from stock_ainalyst import db
import pandas as pd


def market_expected_returns():
    prices = db.get_weekly_market_prices()
    returns = prices.pct_change(fill_method=None).dropna(how="all")
    covs = returns.cov() * 52
    weights = market_weights()
    return 1.7 * covs @ weights


def market_weights():
    market_caps = db.get_market_caps()
    total_market_cap = sum([market_cap for _, market_cap in market_caps])
    market_weights = [(asset_id, market_cap / total_market_cap) for asset_id, market_cap in market_caps]
    market_weights = pd.Series(
        data=[weight for _, weight in market_weights], index=[asset_id for asset_id, _ in market_weights], dtype=float
    )
    return market_weights