from dataclasses import dataclass
from psycopg import Connection


@dataclass
class Company:
    id: int
    name: str
    business_summary: str
    business_raw: str
    business_embedding: list[float]
    stock_id: int


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
