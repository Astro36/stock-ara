import pandas as pd
from pgvector.psycopg import register_vector
import psycopg
import os


conn = psycopg.connect(f"dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}")
# conn = psycopg.connect(f"host=db dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}")
register_vector(conn)


def find_asset_by_id(asset_id: int) -> tuple[int, str, str, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, symbol, exchange, currency FROM assets WHERE id = %s LIMIT 1;",
            (asset_id,),
        )
        res = cur.fetchone()
        return res


def find_stock_by_id(asset_id: int) -> tuple[int, str, int, int, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.id,
                a.name,
                symbol,
                exchange,
                currency,
                outstanding_shares,
                business_summary,
                business_raw
            FROM assets a
                JOIN asset_stocks s ON s.asset_id = a.id
                JOIN companies c ON c.id = s.company_id
            WHERE a.id = %s
            LIMIT 1;
            """,
            (asset_id,),
        )
        res = cur.fetchone()
        return res


def find_stock_by_name(name: str) -> tuple[int, str, int, int, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.id,
                a.name,
                symbol,
                exchange,
                currency,
                outstanding_shares,
                business_summary,
                business_raw
            FROM assets a
                JOIN asset_stocks s ON s.asset_id = a.id
                JOIN companies c ON c.id = s.company_id
            WHERE a.name = %s
            LIMIT 1;
            """,
            (name,),
        )
        res = cur.fetchone()
        return res


def find_stocks_by_keyword(keyword: str, limit=10, offset=0) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.id,
                a.name,
                symbol,
                exchange,
                currency,
                outstanding_shares,
                business_summary,
                business_raw
            FROM assets a
                JOIN asset_stocks s ON s.asset_id = a.id
                JOIN companies c ON c.id = s.company_id
            WHERE business_raw LIKE %s
            ORDER by a.id
            LIMIT %s OFFSET %s;
            """,
            (f"%{keyword}%", limit, offset),
        )
        res = cur.fetchall()
        return res


def find_stocks_by_business(embedding: list[float], limit=5, offset=0) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.id,
                a.name,
                symbol,
                exchange,
                currency,
                outstanding_shares,
                business_summary,
                business_raw
            FROM assets a
                JOIN asset_stocks s ON s.asset_id = a.id
                JOIN companies c ON c.id = s.company_id
            WHERE business_summary IS NOT NULL
            ORDER BY business_embedding <-> %s
            LIMIT %s OFFSET %s;
            """,
            (str(embedding), limit, offset),
        )
        res = cur.fetchall()
        return res


def find_stocks_by_weekly_return_correlation(asset_id, limit=5) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH asset_weekly_close_returns AS (
                SELECT
                    week,
                    asset_id,
                    ((close / lag(close) OVER (PARTITION BY asset_id ORDER BY week) - 1)) AS return
                FROM asset_weekly_close_prices
            ),
            asset_weekly_return_correlations AS (
                SELECT
                    r1.asset_id
                FROM asset_weekly_close_returns r1
                    JOIN asset_weekly_close_returns r2 ON r2.asset_id = %s AND r2.week = r1.week
                GROUP BY r1.asset_id
                ORDER BY corr(stats_agg(r1.return, r2.return)) DESC
                LIMIT %s
            )
            SELECT
                a.id,
                a.name,
                symbol,
                exchange,
                currency,
                outstanding_shares,
                business_summary,
                business_raw
            FROM asset_weekly_return_correlations ac
                JOIN assets a ON a.id = ac.asset_id
                JOIN asset_stocks s ON s.asset_id = ac.asset_id
                JOIN companies c ON c.id = s.company_id
            WHERE business_summary IS NOT NULL;
            """,
            (asset_id, limit + 1),
        )
        res = cur.fetchall()[1:]
        return res


def find_weekly_close_prices_by_id(asset_id):
    with conn.cursor() as cur:
        cur.execute(
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
        )
        res = cur.fetchall()
        return pd.DataFrame(
            data=[price for _, _, price in res],
            index=[date for date, _, _ in res],
            columns=[asset_id],
            dtype=float,
        )


def find_weekly_close_prices_by_exchange(exchange="KOSPI", stock_only=True):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                week,
                p.asset_id,
                close
            FROM asset_weekly_close_prices p
                JOIN assets a ON a.id = p.asset_id
              {'JOIN asset_stocks s ON s.asset_id = a.id' if stock_only else ''}
            WHERE week > now() - '1 year'::interval AND a.exchange = %s
            ORDER BY week;
            """,
            (exchange,),
        )
        res = cur.fetchall()
        prices_dict = {}
        for date, asset_id, price in res:
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


def find_market_caps_by_exchange(exchange="KOSPI"):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH asset_last_prices AS (
                SELECT
                    p.asset_id,
                    last(close, week) as close
                FROM asset_weekly_close_prices p
                    JOIN assets a ON a.id = p.asset_id
                    JOIN asset_stocks s ON s.asset_id = a.id
                WHERE week > now() - '1 year'::interval AND a.exchange = %s
                GROUP BY p.asset_id
            )
            SELECT
                p.asset_id,
                close * outstanding_shares AS market_cap
            FROM asset_last_prices p
                JOIN asset_stocks s ON s.asset_id = p.asset_id
            ORDER BY market_cap DESC;
            """,
            (exchange,),
        )
        res = cur.fetchall()
        return pd.Series(data=[row[1] for row in res], index=[row[0] for row in res], dtype=float)
