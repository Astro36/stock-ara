import OpenDartReader
import pandas as pd

dart = OpenDartReader(api_key="___")
stocks = pd.read_csv("data/stocks.csv", converters={"symbol": str})

# 없는 idx: 67_088980_맥쿼리인프라, 82_011070_LG이노텍, 159_139480_이마트, 189_121600_나노신소재
start_idx = 0
end_idx = 300

for idx, row in stocks.iterrows():
    if idx < start_idx:
        continue
    if idx >= end_idx:
        break

    print(f"{idx}_{row['symbol']}_{row['name']}")
    df = dart.list(row["symbol"], start="2023-01-01", kind="A")
    doc_id = df.iloc[0]["rcept_no"]
    xml_text = dart.document(doc_id)
    with open(f"data/{idx}_{row['symbol']}_{row['name']}.xml", "w", encoding="utf-8") as f:
        f.write(xml_text)
