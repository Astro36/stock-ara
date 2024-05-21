import OpenDartReader
import psycopg


dart = OpenDartReader(api_key="")

start_id = 0
end_id = 1000

conn = psycopg.connect("dbname=demo user=demo password=qwer")

with conn.cursor() as cur:
    cur.execute("SELECT c.id, symbol, c.name FROM companies c JOIN assets a on a.id = c.listed_asset_id;")
    for company_id, asset_symbol, company_name in cur.fetchall():
        if company_id < start_id:
            continue
        if company_id >= end_id:
            break
        symbol = asset_symbol.split('.')[0]
        print(company_id, company_name)
        df = dart.list(symbol, start="2024-01-01", kind="A")
        df = df[df["rm"] == "연"]
        print(df)
        doc_id = df.iloc[0]["rcept_no"]
        xml_text = dart.document(doc_id)
        with open(f"data/dart_filings/{company_id}_{symbol}_{company_name}.xml", "w", encoding="utf-8") as f:
            f.write(xml_text)
