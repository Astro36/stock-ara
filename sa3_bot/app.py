from dotenv import load_dotenv
import psycopg
from openai import OpenAI
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
conn = psycopg.connect("dbname=demo user=demo password=qwer")


def request_openai_gpt_answer(system, user, assistant):
    response = openai_client.chat.completions.create(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
            {"role": "assistant", "content": assistant},
        ],
        model="gpt-3.5-turbo",
    )
    return response.choices[0].message.content.strip()


def request_openai_embedding(text):
    response = openai_client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding


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


def is_relevant_business(query_text, business_report):
    business_summary = request_openai_gpt_answer(
        "You are a corporate analyst. Read the given business report and summarize the business in English by focusing on the parts that match the given query. Write only one sentence.",
        f"Business report: {business_report[:2000]}",
        f"Query: f{query_text}",
    )
    answer = request_openai_gpt_answer(
        """You are a corporate analyst. Read the given business summary and answer "True" or "False" whether the company is matched the given query.""",
        f"Query: f{query_text}",
        f"Business summary: {business_summary}",
    )
    if "true" in answer.lower():
        return (True, business_summary)
    return (False, None)


def find_company_by_business(query_text):
    print("query_text", query_text)
    business_text = request_openai_gpt_answer(
        "You are a corporate analyst. Translate the given query into a sentence about what this company does, as in the example. Print only answer.",
        query_text,
        "Example:\nQ: 무기 만드는 회사를 찾아줘\nA: The company sells defensive weapons.\n\nQ: 반도체와 관련된 회사를 알려줘\nA: The company's main products are semiconductors.\n\nQ: 원자력 발전소를 건설하는 회사\nA: The company builds nuclear power plants.\n\nQ: 이차전지 양극재를 만드는 회사\nA: This company manufactures secondary battery cathode materials.",
    )
    print("business_text", business_text)
    business_embedding = request_openai_embedding(business_text)

    checked_ids = set()
    answers = []
    for asset_id, business_detail in find_similar_business_companies(business_embedding):
        if asset_id not in checked_ids:
            print(business_detail.split('\n')[0])
            answer, reason = is_relevant_business(business_text, business_detail)
            if answer:
                answers.append((asset_id, reason))
            checked_ids.add(asset_id)
    return (business_text, answers)


async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.replace('/search', '').strip()
    if query_text:
        await update.message.reply_text("사업보고서를 조회하고 있습니다. 잠시만 기다려주세요(30초).")
        (business, companies) = find_company_by_business(update.message.text)
        answer = []
        for asset_id, reason in companies:
            (_, symbol, name, _, _) = find_asset_by_id(asset_id)
            answer.append(f"<b>{name}({symbol})</b>\n{reason}")
        answer = "\n\n".join(answer)
        await update.message.reply_text(f"<i>Query: {business}</i>\n\n{answer}", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/search 반도체 장비 회사</code>와 같이 찾으려는 기업이 영위하는 사업을 알려주세요.", parse_mode="HTML")


if __name__ == "__main__":
    load_dotenv()

    app = ApplicationBuilder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()
    app.add_handler(CommandHandler("search", search))
    app.run_polling()
