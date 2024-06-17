import pandas as pd
from psycopg import Connection


class AssetPriceRepository:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def get_asset_prices(self, asset_id: int):
        records = self.conn.execute(
            """
            SELECT
                week,
                asset_id,
                close
            FROM asset_weekly_close_prices
            WHERE week > now() - '1 year'::interval AND asset_id = %s
            ORDER BY week;
            """,
            (asset_id,),
        ).fetchall()

        return pd.DataFrame(
            data=[price for _, _, price in records],
            index=[date for date, _, _ in records],
            columns=[asset_id],
            dtype=float,
        )

    def get_all_stock_prices(self, exchange: str):
        records = self.conn.execute(
            f"""
            SELECT
                week,
                p.asset_id,
                close
            FROM asset_weekly_close_prices p
                JOIN assets a ON a.id = p.asset_id
                JOIN asset_stocks s ON s.asset_id = a.id
            WHERE week > now() - '1 year'::interval AND a.exchange = %s
            ORDER BY week;
            """,
            (exchange,),
        ).fetchall()
        prices_dict = {}
        for date, asset_id, price in records:
            if asset_id in prices_dict:
                prices_dict[asset_id].append((date, price))
            else:
                prices_dict[asset_id] = [(date, price)]
        prices_df = []
        for asset_id, prices in prices_dict.items():
            prices_df.append(
                pd.DataFrame(
                    data=[price for _, price in prices],
                    index=[date for date, _ in prices],
                    columns=[asset_id],
                    dtype=float,
                )
            )
        return pd.concat(prices_df, axis=1)

    def get_all_stock_market_caps(self, exchange: str):
        records = self.conn.execute(
            """
            SELECT
                asset_id,
                market_cap
            FROM stock_market_caps m
                JOIN assets a ON a.id = m.asset_id
            WHERE exchange = %s
            ORDER BY market_cap DESC;
            """,
            (exchange,),
        ).fetchall()
        return pd.Series(
            data=[market_cap for _, market_cap in records],
            index=[asset_id for asset_id, _ in records],
            dtype=float,
        )
