import pandas as pd

df = pd.read_csv("___.csv")
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

df.to_csv("stocks.csv", index=False)
