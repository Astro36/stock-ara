import pandas as pd
from psycopg import Connection
from stock_ara.domain.company import Company


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


class CompanyRepository:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def find_by_id(self, id: int) -> Company:
        record = self.conn.execute(
            """
            SELECT
                id,
                name,
                business_summary,
                business,
                business_embedding,
                asset_id
            FROM companies c
                JOIN company_filings f ON f.company_id = c.id
                JOIN asset_stocks s ON s.company_id = c.id
            WHERE id = %s
            LIMIT 1;
            """,
            (id,),
        ).fetchone()

        if record is None:
            return Exception()

        return Company(
            id=record[0],
            name=record[1],
            business_summary=record[2],
            business_raw=record[3],
            business_embedding=record[4],
            stock_id=record[5],
        )

    def find_by_name(self, name: str) -> Company:
        record = self.conn.execute(
            """
            SELECT
                id,
                name,
                business_summary,
                business,
                business_embedding,
                asset_id
            FROM companies c
                JOIN company_filings f ON f.company_id = c.id
                JOIN asset_stocks s ON s.company_id = c.id
            WHERE name = %s
            LIMIT 1;
            """,
            (name,),
        ).fetchone()

        if record is None:
            return Exception()

        return Company(
            id=record[0],
            name=record[1],
            business_summary=record[2],
            business_raw=record[3],
            business_embedding=record[4],
            stock_id=record[5],
        )

    def find_by_stock_id(self, stock_id: int) -> Company:
        record = self.conn.execute(
            """
            SELECT
                id,
                name,
                business_summary,
                business,
                business_embedding,
                asset_id
            FROM companies c
                JOIN company_filings f ON f.company_id = c.id
                JOIN asset_stocks s ON s.company_id = c.id
            WHERE asset_id = %s
            LIMIT 1;
            """,
            (stock_id,),
        ).fetchone()

        if record is None:
            return Exception()

        return Company(
            id=record[0],
            name=record[1],
            business_summary=record[2],
            business_raw=record[3],
            business_embedding=record[4],
            stock_id=record[5],
        )

    def find_all_by_business(self, business_embedding: str, limit=10, offset=0) -> Company:
        records = self.conn.execute(
            """
            SELECT
                id,
                name,
                business_summary,
                business,
                business_embedding,
                asset_id
            FROM companies c
                JOIN company_filings f ON f.company_id = c.id
                JOIN asset_stocks s ON s.company_id = c.id
            WHERE business_summary IS NOT NULL
            ORDER BY business_embedding <-> %s
            LIMIT %s OFFSET %s;
            """,
            (str(business_embedding), limit, offset),
        ).fetchall()

        return [
            Company(
                id=record[0],
                name=record[1],
                business_summary=record[2],
                business_raw=record[3],
                business_embedding=record[4],
                stock_id=record[5],
            )
            for record in records
        ]

    def find_all_by_keyword(self, keyword: str, limit=10, offset=0) -> Company:
        records = self.conn.execute(
            """
            SELECT
                id,
                name,
                business_summary,
                business,
                business_embedding,
                s.asset_id
            FROM companies c
                JOIN company_filings f ON f.company_id = c.id
                JOIN asset_stocks s ON s.company_id = c.id
                JOIN stock_market_caps m ON m.asset_id = s.asset_id
            WHERE business LIKE %s
            ORDER BY market_cap DESC
            LIMIT %s OFFSET %s;
            """,
            (f"%{keyword}%", limit, offset),
        ).fetchall()

        return [
            Company(
                id=record[0],
                name=record[1],
                business_summary=record[2],
                business_raw=record[3],
                business_embedding=record[4],
                stock_id=record[5],
            )
            for record in records
        ]
