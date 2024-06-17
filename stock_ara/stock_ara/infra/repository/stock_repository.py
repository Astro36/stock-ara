from psycopg import Connection
from stock_ara.domain.stock import Stock


class StockRepository:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def find_by_id(self, id: int) -> Stock:
        record = self.conn.execute(
            """
            SELECT
                id,
                name,
                symbol,
                exchange,
                currency,
                company_id
            FROM assets a
                JOIN asset_stocks s ON s.asset_id = a.id
            WHERE id = %s
            LIMIT 1;
            """,
            (id,),
        ).fetchone()

        if record is None:
            return Exception()

        return Stock(
            id=record[0],
            name=record[1],
            symbol=record[2],
            exchange=record[3],
            currency=record[4],
            company_id=record[5],
        )

    def find_by_name(self, name: str) -> Stock:
        record = self.conn.execute(
            """
            SELECT
                id,
                name,
                symbol,
                exchange,
                currency,
                company_id
            FROM assets a
                JOIN asset_stocks s ON s.asset_id = a.id
            WHERE name = %s
            LIMIT 1;
            """,
            (name,),
        ).fetchone()

        if record is None:
            return Exception()

        return Stock(
            id=record[0],
            name=record[1],
            symbol=record[2],
            exchange=record[3],
            currency=record[4],
            company_id=record[5],
        )

    def find_all_by_correlation(self, id: int, limit=5, threshold=0.7) -> list[Stock]:
        records = self.conn.execute(
            """
            WITH asset_weekly_returns AS (
                SELECT
                    week,
                    p.asset_id,
                    ((close / lag(close) OVER (PARTITION BY p.asset_id ORDER BY week) - 1)) AS return
                FROM asset_weekly_close_prices p
                    JOIN asset_stocks s ON s.asset_id = p.asset_id
            ),
            asset_correlations AS (
                SELECT
                    r1.asset_id,
                    corr(stats_agg(r1.return, r2.return)) AS correlation
                FROM asset_weekly_returns r1
                    JOIN asset_weekly_returns r2 ON r2.asset_id = %s AND r2.week = r1.week
                GROUP BY r1.asset_id
                ORDER BY correlation DESC
                LIMIT %s
            )
            SELECT
                id,
                name,
                symbol,
                exchange,
                currency,
                company_id
            FROM asset_correlations ac
                JOIN assets a ON a.id = ac.asset_id
                JOIN asset_stocks s ON s.asset_id = ac.asset_id
            WHERE correlation > %s
            """,
            (id, limit, threshold),
        ).fetchall()

        return [
            Stock(
                id=record[0],
                name=record[1],
                symbol=record[2],
                exchange=record[3],
                currency=record[4],
                company_id=record[5],
            )
            for record in records
        ]
