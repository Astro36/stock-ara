import glob
import os
import pandas as pd
from pgvector.psycopg import register_vector
import psycopg
import re
from stock_ainalyst.llm import openai


conn = psycopg.connect("dbname=demo user=demo password=qwer")
register_vector(conn)


filings = {}

for filepath in glob.glob("data/dart_filings/*.xml"):
    filename = os.path.basename(filepath)
    print(filename)
    asset_symbol = filename.split("_")[1]
    with open(filepath, "r", encoding="utf-8") as f1:
        content = f1.read()

        i = content.find("L-0-2-1-L")
        j = content.find("L-0-2-3-L")
        content = content[i:j]

        content = re.sub("</?[^>]*>", "", content)
        content = re.sub("\s+", " ", content)

        i = content.find(">") + 1
        j = content.rfind("<")
        content = content[i:j].strip()

        content = filename.split(".")[0].split("_")[2] + "\n" + content

        filings[asset_symbol] = content


with conn.cursor() as cur:
    cur.execute(
        """
        SELECT
            symbol,
            a.name
        FROM assets a
            JOIN asset_stocks s ON s.asset_id = a.id
            JOIN companies c ON c.id = s.company_id
        WHERE business_raw IS NULL;
        """
    )
    res = cur.fetchall()

    for symbol, name in res:
        symbol_only = symbol.split(".")[0]
        if symbol_only in filings:
            print(f"Insert {name}")
            cur.execute(
                """
                UPDATE companies SET business_raw = %s WHERE companies.id = (SELECT s.company_id FROM asset_stocks s JOIN assets a ON a.id = s.asset_id WHERE symbol = %s);
                """,
                (filings[symbol_only], symbol),
            )
    conn.commit()


with conn.cursor() as cur:
    cur.execute(
        """
        SELECT
            a.id,
            a.name,
            business_raw
        FROM assets a
            JOIN asset_stocks s ON s.asset_id = a.id
            JOIN companies c ON c.id = s.company_id
        WHERE (business_summary IS NULL OR length(business_summary) <= 100)
            AND business_raw IS NOT NULL
            AND length(business_raw) >= 100
        ORDER BY a.id;
        """
    )
    res = cur.fetchall()

    for idx, (asset_id, name, business_raw) in enumerate(res):
        print(idx, name)
        business_summary = openai.request_gpt_answer(
            [
                f"You are a corperate analyst. Read the given business report excerpt and summarize it in English sentences of 800 characters or less, focusing on the company's specific business details that can distinguish it from other companies, such as product name and sales proportion.\nConditions of writing: Answer in English, but do not translate company's names(ex. {name}) into English. Don't write about market and company forecasts. Write it in a sentence form, not a list. The amount is marked as a ratio in sales, etc. instead of the won.",
                f'Business report: """{business_raw[:3000]}"""',
            ],
            model="gpt-3.5-turbo",
        )
        cur.execute(
            """
            UPDATE companies SET business_summary = %s WHERE companies.id = (SELECT company_id FROM asset_stocks WHERE asset_id = %s);
            """,
            (business_summary, asset_id),
        )
        conn.commit()


with conn.cursor() as cur:
    cur.execute(
        """
        SELECT
            a.id,
            a.name,
            business_summary
        FROM assets a
            JOIN asset_stocks s ON s.asset_id = a.id
            JOIN companies c ON c.id = s.company_id
        WHERE business_summary IS NOT NULL
            AND business_embedding IS NULL
        ORDER BY a.id;
        """
    )
    res = cur.fetchall()

    for idx, (asset_id, name, business_summary) in enumerate(res):
        print(idx, name)
        business_embedding = openai.request_embedding(business_summary)
        cur.execute(
            """
            UPDATE companies SET business_embedding = %s WHERE companies.id = (SELECT company_id FROM asset_stocks WHERE asset_id = %s);
            """,
            (str(business_embedding), asset_id),
        )
        conn.commit()
