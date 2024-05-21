import pandas as pd
import psycopg


conn = psycopg.connect("dbname=demo user=demo password=qwer")

df = pd.read_csv("data/data_1918_20240521.csv")
df = df[~df["소속부"].str.contains("소속부없음", na=False)]
df = df[df["시장구분"] != "KONEX"]
df = df[df["종목코드"].str[-1] == "0"]

df = df[["종목코드", "종목명", "시장구분", "시가총액", "상장주식수"]]
df.sort_values(by=["시가총액"], ascending=False, inplace=True)
df.rename(
    columns={
        "종목코드": "symbol",
        "종목명": "name",
        "시장구분": "exchange",
        "시가총액": "marketCap",
        "상장주식수": "sharesOutstanding",
    },
    inplace=True,
)

with conn.cursor() as cur:
    cur.execute(
        """
        INSERT INTO assets (symbol, name, currency, type) VALUES ('^KS11', 'KOSPI', 'KRW', 'index') ON CONFLICT DO NOTHING;
        INSERT INTO assets (symbol, name, currency, type) VALUES ('^KQ11', 'KOSDAQ', 'KRW', 'index') ON CONFLICT DO NOTHING;
        """,
    )
    conn.commit()

    for idx, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO assets (symbol, name, currency, type) VALUES (%(symbol)s, %(name)s, 'KRW', 'stock') ON CONFLICT DO NOTHING;
            """,
            {
                "symbol": f'{row["symbol"]}.{"KS" if row["exchange"] == "KOSPI" else "KQ"}',
                "name": row["name"],
            },
        )
    conn.commit()

    for idx, row in df.iterrows():
        cur.execute(
            """
            INSERT INTO companies (name, listed_asset_id, outstanding_shares) VALUES (%(name)s, (SELECT id FROM assets WHERE name = %(name)s), %(outstanding_shares)s);
            """,
            {
                "name": row["name"],
                "outstanding_shares": row["sharesOutstanding"],
            },
        )
    conn.commit()
