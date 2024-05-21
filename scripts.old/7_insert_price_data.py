import glob
import os
import pandas as pd
import psycopg2
import psycopg2.extras
import yfinance as yf

conn = psycopg2.connect("dbname=demo user=demo password=qwer")
cur = conn.cursor()

stocks = pd.read_csv("data/stocks2.csv", converters={"symbol": str})

for idx, row in stocks.iterrows():
    if idx >= 296:
        break

    symbol = row["symbol"]
    exchange = "KS" if row["exchange"] == "KOSPI" else "KQ"

    print(idx, f"{symbol}.{exchange}")
    prices = yf.download(f"{symbol}.{exchange}", start="2020-01-01")

    cur.execute(
        "INSERT INTO securities(symbol, exchange, currency, company_id) VALUES (%s, %s, 'KRW', (SELECT id FROM companies WHERE name = %s))",
        (symbol, exchange, row["name"]),
    )
    conn.commit()

    sql = "INSERT INTO security_prices (date, security_id, open, high, low, close, volume) VALUES %s;"
    argslist = prices[["Open", "High", "Low", "Close", "Volume"]].reset_index().values.tolist()
    template = f"(%s, (SELECT id FROM securities WHERE symbol = '{symbol}'), %s, %s, %s, %s, %s)"
    psycopg2.extras.execute_values(cur, sql, argslist, template)
    conn.commit()
