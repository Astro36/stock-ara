import pandas as pd
from pgvector.psycopg import register_vector
import psycopg


conn = psycopg.connect("dbname=demo user=demo password=qwer")
register_vector(conn)


def find_asset_by_id(asset_id: int) -> tuple[int, str, str, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, symbol, name, currency, type FROM assets WHERE id = %s LIMIT 1;",
            (asset_id,),
        )
        res = cur.fetchone()
        return res


def find_company_by_asset_id(asset_id: int) -> tuple[int, str, int, int, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, listed_asset_id, outstanding_shares, business_summary, business_detail FROM companies WHERE listed_asset_id = %s LIMIT 1;",
            (asset_id,),
        )
        res = cur.fetchone()
        return res


def find_company_by_name(name: str) -> tuple[int, str, int, int, str, str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, name, listed_asset_id, outstanding_shares, business_summary, business_detail FROM companies WHERE name = %s LIMIT 1;",
            (name,),
        )
        res = cur.fetchone()
        return res


def find_asset_ids_by_business(embedding: list[float], limit=5) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT listed_asset_id FROM companies ORDER BY business_embedding <-> %s LIMIT %s;",
            (str(embedding), limit),
        )
        res = cur.fetchall()
        return [row[0] for row in res]


def find_asset_ids_by_price_change_correlation(asset_id, limit=5) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH asset_prices_weekly AS (
                SELECT
                    time_bucket('1 week'::interval, date) AS week,
                    asset_id,
                    last(close, date) AS close
                FROM asset_prices
                GROUP BY week, asset_id
            ),
            asset_price_changes AS (
                SELECT week, asset_id, ((close / lag(close) OVER (PARTITION BY asset_id ORDER BY week) - 1)) AS change
                FROM asset_prices_weekly
            )
            SELECT pc1.asset_id
            FROM asset_price_changes pc1
            JOIN asset_price_changes pc2 ON pc2.asset_id = %s AND pc1.week = pc2.week
            GROUP BY pc1.asset_id
            ORDER BY corr(stats_agg(pc1.change, pc2.change)) DESC
            LIMIT %s;
            """,
            (asset_id, limit + 1),
        )
        res = cur.fetchall()[1:]
        return [row[0] for row in res]


def find_weekly_asset_prices_by_asset_id(asset_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                time_bucket('1 week'::interval, date) AS week,
                last(close, date)
            FROM asset_prices p
            WHERE date > now() - '1 year'::interval AND asset_id = %s
            GROUP BY week
            ORDER BY week
            """,
            (asset_id,),
        )
        res = cur.fetchall()
        return pd.DataFrame(
            data=[price for _, price in res],
            index=[date for date, _ in res],
            columns=[asset_id],
            dtype=float,
        )


def find_weekly_asset_prices_by_market(market="KS"):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                time_bucket('1 week'::interval, date) AS week,
                asset_id,
                last(close, date)
            FROM asset_prices p JOIN assets a ON a.id = p.asset_id
            WHERE date > now() - '1 year'::interval AND a.symbol LIKE %s
            GROUP BY week, asset_id
            ORDER BY asset_id, week
            """,
            (f"%.{market}",),
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


def find_market_caps_by_market(market="KS"):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH asset_prices_close AS (
                SELECT
                    asset_id,
                    last(close, date) AS close
                FROM asset_prices p JOIN assets a ON a.id = p.asset_id
                WHERE a.symbol LIKE %s
                GROUP BY asset_id
            )
            SELECT
                asset_id,
                close * outstanding_shares AS market_cap
            FROM companies c JOIN asset_prices_close p on p.asset_id = c.listed_asset_id
            ORDER BY market_cap DESC;
            """,
            (f"%.{market}",),
        )
        res = cur.fetchall()
        return pd.Series(data=[row[1] for row in res], index=[row[0] for row in res], dtype=float)
