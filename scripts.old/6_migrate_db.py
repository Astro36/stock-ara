from chromadb import HttpClient
import glob
import os
import psycopg2

client = HttpClient()
collection = client.get_collection("business")
db = collection.get()

conn = psycopg2.connect("dbname=demo user=demo password=qwer")
cur = conn.cursor()

for row in db["ids"]:
    cur.execute(f"INSERT INTO companies(name) VALUES ('{row}');")
conn.commit()

for filepath in glob.glob("data/*.txt"):
    filename = os.path.basename(filepath)

    print(filename.split(".")[0])
    with open(filepath, "r", encoding="utf-8") as f1:
        business_detail = f1.read()

        symbol = filename.split(".")[0].split("_")[1]
        name = filename.split(".")[0].split("_")[2]

        data = collection.get([name], include=["embeddings", "documents"])
        embedding = data["embeddings"][0]
        business_summary = data["documents"][0]

        cur.execute(
            f"INSERT INTO company_filings(company_id, date, business_summary, business_detail, business_embedding) VALUES ((SELECT id FROM companies WHERE name='{name}'), NOW(), %s, %s, %s);",
            (business_summary, business_detail, embedding),
        )
        conn.commit()
