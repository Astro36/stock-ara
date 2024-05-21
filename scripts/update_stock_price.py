import psycopg
import yfinance as yf


conn = psycopg.connect("dbname=demo user=demo password=qwer")

with conn.cursor() as cur:
    cur.execute("SELECT id, symbol, name FROM assets WHERE type = 'stock' OR type = 'index';")
    for asset_id, asset_symbol, asset_name in cur.fetchall():
        print(asset_id, asset_name)
        prices = yf.download(f"{asset_symbol}", start="2020-01-01")
        for row in prices[["Open", "High", "Low", "Close", "Volume"]].reset_index().values.tolist():
            cur.execute(
                "INSERT INTO asset_prices (date, asset_id, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;",
                (row[0], asset_id, row[1], row[2], row[3], row[4], row[5]),
            )
        conn.commit()
