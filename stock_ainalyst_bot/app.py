from dotenv import load_dotenv
import psycopg
from openai import OpenAI
import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
conn = psycopg.connect("dbname=demo user=demo password=qwer")


# === GPT ===
def request_openai_gpt_answer(system, user, assistant):
    response = openai_client.chat.completions.create(
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
            {"role": "assistant", "content": assistant},
        ],
        model="gpt-4o",
    )
    return response.choices[0].message.content.strip()


def request_openai_embedding(text):
    response = openai_client.embeddings.create(input=[text], model="text-embedding-3-small")
    return response.data[0].embedding


# === DB ===
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


def find_similar_price_trend_companies(asset_id):
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH asset_price_weekly AS (
                SELECT
                    time_bucket('1 week'::interval, date) AS week,
                    asset_id,
                    close(candlestick_agg(date, close, volume)) AS close
                FROM asset_prices
                GROUP BY week, asset_id
            ),
            asset_price_changes AS (
                SELECT week, asset_id, ((close / LAG(close) OVER (PARTITION BY asset_id ORDER BY week) - 1)) AS change
                FROM asset_price_weekly
            )
            SELECT
                pc1.asset_id,
                (SELECT business_detail FROM companies c WHERE c.listed_asset_id = pc1.asset_id)
            FROM asset_price_changes pc1
            JOIN asset_price_changes pc2 ON pc2.asset_id = %s AND pc1.week = pc2.week
            GROUP BY pc1.asset_id
            ORDER BY corr(stats_agg(pc1.change, pc2.change)) DESC
            LIMIT 10;
            """,
            (asset_id,),
        )
        res = cur.fetchall()
        return [row for row in res if row[1]]


def calculate_beta(asset_ids):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH asset_price_weekly AS (
                SELECT
                    time_bucket('1 week'::interval, date) AS week,
                    asset_id,
                    close(candlestick_agg(date, close, volume)) AS close
                FROM asset_prices
                WHERE date > now() - '1 year'::interval
                GROUP BY week, asset_id
            ),
            asset_price_changes AS (
                SELECT week, asset_id, ((close / LAG(close) OVER (PARTITION BY asset_id ORDER BY week) - 1)) AS change
                FROM asset_price_weekly
            )
            SELECT
                pc1.asset_id,
                slope(stats_agg(pc1.change, pc2.change)) AS beta
            FROM asset_price_changes pc1
            JOIN asset_price_changes pc2 ON pc2.asset_id = (
                CASE WHEN (SELECT symbol FROM assets WHERE id = pc1.asset_id) LIKE '%%.KS' THEN (SELECT id FROM assets WHERE symbol = '^KS11')
                    ELSE (SELECT id FROM assets WHERE symbol = '^KQ11')
                END
            ) AND pc1.week = pc2.week
            WHERE pc1.asset_id IN ({",".join(map(str, asset_ids))})
            GROUP BY pc1.asset_id
            ORDER BY beta DESC;
            """,
        )
        res = cur.fetchall()
        return {asset_id: beta for asset_id, beta in res}


# === LLM ===
def is_relevant_business(query_text, business_report):
    business_summary = request_openai_gpt_answer(
        "You are a corporate analyst. Read the given business report, extract the parts relevant to the given query in Korean. Write only one sentence.",
        f"Query: f{query_text}",
        f"Business report: {business_report[:2000]}",
    )
    print(business_summary)
    answer = request_openai_gpt_answer(
        """You are a corporate analyst. Read the given business report summary and answer "True" or "False" whether the company is relevant to the given query.""",
        f"Query: f{query_text}",
        f"Business report summary: {business_summary}",
    )
    if "true" in answer.lower():
        return (True, business_summary)
    return (False, None)


def find_company_by_business(query_text):
    print("query_text", query_text)
    business_text = request_openai_gpt_answer(
        "You are a corporate analyst. Translate the given query into an English sentence about what this company does in detailed, as in the example. Print only answer.",
        query_text,
        "Example:\nQ: 무기 만드는 회사를 찾아줘\nA: The company sells defensive weapons.\n\nQ: 반도체와 관련된 회사를 알려줘\nA: The company's main products are semiconductors.\n\nQ: 원자력 발전소를 건설하는 회사\nA: The company builds nuclear power plants.\n\nQ: 이차전지 양극재를 만드는 회사\nA: This company manufactures secondary battery cathode materials.\n\nQ: 면역항암제\n: This company's main products are cancer immunotherapies.\n\n: 물류: This company is logistics company.",
    )
    business_keyword = request_openai_gpt_answer(
        "What is the noun keyword of the given sentence about the business of a company. Print only answer.",
        business_text,
        "Example:\nQ: The company manufactures equipment used for the back-end processes of semiconductor production.\nA: Semiconductor back-end process.\n\nQ: This company is involved in the cosmetics industry.\nA: Cosmetics",
    )
    business_description_text = request_openai_gpt_answer(
        "Describe the given keyword in 100 characters or less. Print only answer.",
        business_keyword,
        "Example:\nQ: Semiconductor back-end process.\nA: The semiconductor back-end process involves assembly and testing of chips after wafer fabrication.",
    )
    business_text = business_text + " " + business_description_text  # CoT
    print("business_text", business_text)
    business_embedding = request_openai_embedding(business_text)

    checked_ids = set()
    answers = []
    k = 5
    t = 20
    for asset_id, business_detail in find_similar_business_companies(business_embedding):
        if asset_id not in checked_ids and t > 0:
            checked_ids.add(asset_id)
            print(business_detail.split("\n")[0])
            answer, reason = is_relevant_business(business_text, business_detail)
            t -= 1
            if answer:
                answers.append((asset_id, reason))
                for asset_id2, business_detail2 in find_similar_price_trend_companies(asset_id)[:k]:
                    if asset_id2 not in checked_ids and t > 0:
                        checked_ids.add(asset_id2)
                        print(business_detail2.split("\n")[0])
                        answer2, reason2 = is_relevant_business(business_text, business_detail2)
                        t -= 1
                        if answer2:
                            answers.append((asset_id2, reason2))
            else:
                k -= 1
    return (business_text, answers)


# === Bot ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("주식 스크리닝 봇입니다. 관리자(@psj1026)가 서버를 켰을 때만 동작합니다.")


async def search_business(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.replace("/business", "").strip()
    print(query_text)
    if query_text:
        await update.message.reply_text("사업보고서를 조회하고 있습니다. 잠시만 기다려주세요(30초).")
        (business, companies) = find_company_by_business(update.message.text)
        if len(companies) > 0:
            betas = calculate_beta([asset_id for asset_id, _ in companies])
            sorted_companies = sorted(companies, key=lambda company: betas[company[0]], reverse=True)

            answer = []
            for asset_id, reason in sorted_companies:
                (_, symbol, name, _, _) = find_asset_by_id(asset_id)
                answer.append(f"<b>{name}({symbol})</b>\n{reason}\n52주 베타: {betas[asset_id]:.2f}")
            answer = "\n\n".join(answer)
            await update.message.reply_text(
                f"<i>Query: {business}</i>\n\n{answer}\n\n검색결과가 만족스럽지 않다면 영어로 검색해주세요.",
                parse_mode="HTML",
            )
        else:
            await update.message.reply_text(
                f"<i>Query: {business}</i>\n\n관련 기업이 없습니다. 검색결과가 만족스럽지 않다면 영어로 검색해주세요.",
                parse_mode="HTML",
            )
    else:
        await update.message.reply_text(
            "<code>/search 반도체 장비 회사</code>와 같이 찾으려는 기업이 영위하는 사업을 알려주세요.",
            parse_mode="HTML",
        )


if __name__ == "__main__":
    load_dotenv()

    app = ApplicationBuilder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("business", search_business))
    app.run_polling()
