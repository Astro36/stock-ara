import cvxpy as cp
import math
import pandas as pd
from stock_ara.domain.asset import Asset
from stock_ara.infra.database import asset_price_repository, cache_repository


class PortfolioManager:
    def make_optimal_portfolio(self, assets: Asset, risk_free_rate=0.035) -> list[tuple[Asset, float]]:
        prices = pd.concat([asset_price_repository.get_asset_prices(asset.id) for asset in assets], axis=1)
        returns = prices.pct_change(fill_method=None).dropna(how="all")
        covs = returns.cov().fillna(0) * 52

        expected_returns = pd.Series([cache_repository.get_implied_expected_return(asset.id) for asset in assets], index=[asset.id for asset in assets], dtype=float).to_numpy()
        weights = cp.Variable(len(assets))
        k = cp.Variable()

        prob = cp.Problem(
            cp.Minimize(cp.quad_form(weights, covs)),
            [
                (expected_returns - risk_free_rate).T @ weights == 1,
                cp.sum(weights) == k,
                k >= 0,
                weights >= 0,
            ],
        )
        prob.solve()

        weights = weights.value / k.value

        portfolio = [
            (
                assets[idx],
                weight,
                cache_repository.get_implied_expected_return(assets[idx].id),
                math.sqrt(covs.iloc[idx, idx]),
            )
            for idx, weight in enumerate(weights)
        ]
        return portfolio
