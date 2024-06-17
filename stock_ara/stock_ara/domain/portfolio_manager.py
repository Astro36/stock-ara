import cvxpy as cp
import math
import pandas as pd
from stock_ara.domain.asset import Asset
from stock_ara.infra.database import asset_price_repository, cache_repository


class PortfolioManager:
    def make_optimal_portfolio(self, assets: Asset) -> list[tuple[Asset, float]]:
        asset_prices = pd.concat([asset_price_repository.get_asset_prices(asset.id) for asset in assets], axis=1)
        asset_returns = asset_prices.pct_change(fill_method=None).dropna(how="all")
        asset_covs = asset_returns.cov().fillna(0) * 52
        asset_expected_returns = pd.Series([cache_repository.get_implied_expected_return(asset.id) for asset in assets], index=[asset.id for asset in assets], dtype=float)
        asset_expected_returns = asset_expected_returns.to_numpy()

        portfolio_weights = cp.Variable(len(assets))
        portfolio_return = asset_expected_returns.T @ portfolio_weights
        portfolio_risk = cp.quad_form(portfolio_weights, asset_covs)
        prob = cp.Problem(cp.Maximize(portfolio_return - portfolio_risk), [cp.sum(portfolio_weights) == 1, portfolio_weights >= 0])
        prob.solve()

        portfolio = [
            (
                assets[idx],
                weight,
                cache_repository.get_implied_expected_return(assets[idx].id),
                math.sqrt(asset_covs.iloc[idx, idx]),
            )
            for idx, weight in enumerate(portfolio_weights.value)
        ]
        return portfolio
