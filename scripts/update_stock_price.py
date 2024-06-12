import psycopg
import yfinance as yf


conn = psycopg.connect("dbname=demo user=demo password=qwer")

with conn.cursor() as cur:
    cur.execute("SELECT id, name, symbol FROM assets")
    for asset_id, asset_name, asset_symbol in cur.fetchall():
        print(asset_id, asset_name)
        prices = yf.download(f"{asset_symbol}", start="2024-04-01")
        for row in prices[["Open", "High", "Low", "Close", "Volume"]].reset_index().values.tolist():
            cur.execute(
                "INSERT INTO asset_prices (date, asset_id, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (row[0], asset_id, row[1], row[2], row[3], row[4], row[5]),
            )
        conn.commit()
