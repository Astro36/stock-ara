import os
import psycopg


conn = psycopg.connect("dbname=demo user=demo password=qwer")


def find_asset_by_id(asset_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, symbol, name, currency, type FROM assets WHERE id = %s LIMIT 1;",
            (asset_id,),
        )
        res = cur.fetchone()
        return res


def find_similar_business_companies(embedding):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT listed_asset_id, business_detail FROM companies ORDER BY business_embedding <-> %s LIMIT 10;",
            (str(embedding),),
        )
        res = cur.fetchall()
        return res


def find_similar_price_trend_companies(asset_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH asset_price_weekly AS (
                SELECT
                    time_bucket('1 week'::interval, date) AS week,
                    asset_id,
                    close(candlestick_agg(date, close, volume)) AS close
                FROM asset_prices
                GROUP BY week, asset_id
            ),
            asset_price_changes AS (
                SELECT week, asset_id, ((close / LAG(close) OVER (PARTITION BY asset_id ORDER BY week) - 1)) AS change
                FROM asset_price_weekly
            )
            SELECT
                pc1.asset_id,
                (SELECT business_detail FROM companies c WHERE c.listed_asset_id = pc1.asset_id)
            FROM asset_price_changes pc1
            JOIN asset_price_changes pc2 ON pc2.asset_id = %s AND pc1.week = pc2.week
            GROUP BY pc1.asset_id
            ORDER BY corr(stats_agg(pc1.change, pc2.change)) DESC
            LIMIT 10;
            """,
            (asset_id,),
        )
        res = cur.fetchall()
        return [row for row in res if row[1]]


def calculate_beta(asset_ids):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH asset_price_weekly AS (
                SELECT
                    time_bucket('1 week'::interval, date) AS week,
                    asset_id,
                    close(candlestick_agg(date, close, volume)) AS close
                FROM asset_prices
                WHERE date > now() - '1 year'::interval
                GROUP BY week, asset_id
            ),
            asset_price_changes AS (
                SELECT week, asset_id, ((close / LAG(close) OVER (PARTITION BY asset_id ORDER BY week) - 1)) AS change
                FROM asset_price_weekly
            )
            SELECT
                pc1.asset_id,
                slope(stats_agg(pc1.change, pc2.change)) AS beta
            FROM asset_price_changes pc1
            JOIN asset_price_changes pc2 ON pc2.asset_id = (
                CASE WHEN (SELECT symbol FROM assets WHERE id = pc1.asset_id) LIKE '%%.KS' THEN (SELECT id FROM assets WHERE symbol = '^KS11')
                    ELSE (SELECT id FROM assets WHERE symbol = '^KQ11')
                END
            ) AND pc1.week = pc2.week
            WHERE pc1.asset_id IN ({",".join(map(str, asset_ids))})
            GROUP BY pc1.asset_id
            ORDER BY beta DESC;
            """,
        )
        res = cur.fetchall()
        return {asset_id: beta for asset_id, beta in res}
