from dotenv import load_dotenv
import os
from stock_ainalyst import db, llm
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


# === LLM ===
def is_relevant_business(query_text, business_report):
    business_summary = llm.request_openai_gpt_answer(
        "You are a corporate analyst. Read the given business report, extract the parts relevant to the given query in Korean. Write only one sentence.",
        f"Query: f{query_text}",
        f"Business report: {business_report[:2000]}",
    )
    print(business_summary)
    answer = llm.request_openai_gpt_answer(
        """You are a corporate analyst. Read the given business report summary and answer "True" or "False" whether the company is relevant to the given query.""",
        f"Query: f{query_text}",
        f"Business report summary: {business_summary}",
    )
    if "true" in answer.lower():
        return (True, business_summary)
    return (False, None)


def find_company_by_business(query_text):
    print("query_text", query_text)
    business_text = llm.request_openai_gpt_answer(
        "You are a corporate analyst. Translate the given query into an English sentence about what this company does in detailed, as in the example. Print only answer.",
        query_text,
        "Example:\nQ: 무기 만드는 회사를 찾아줘\nA: The company sells defensive weapons.\n\nQ: 반도체와 관련된 회사를 알려줘\nA: The company's main products are semiconductors.\n\nQ: 원자력 발전소를 건설하는 회사\nA: The company builds nuclear power plants.\n\nQ: 이차전지 양극재를 만드는 회사\nA: This company manufactures secondary battery cathode materials.\n\nQ: 면역항암제\n: This company's main products are cancer immunotherapies.\n\n: 물류: This company is logistics company.",
    )
    business_keyword = llm.request_openai_gpt_answer(
        "What is the noun keyword of the given sentence about the business of a company. Print only answer.",
        business_text,
        "Example:\nQ: The company manufactures equipment used for the back-end processes of semiconductor production.\nA: Semiconductor back-end process.\n\nQ: This company is involved in the cosmetics industry.\nA: Cosmetics",
    )
    business_description_text = llm.request_openai_gpt_answer(
        "Describe the given keyword in 100 characters or less. Print only answer.",
        business_keyword,
        "Example:\nQ: Semiconductor back-end process.\nA: The semiconductor back-end process involves assembly and testing of chips after wafer fabrication.",
    )
    business_text = business_text + " " + business_description_text  # CoT
    print("business_text", business_text)
    business_embedding = llm.request_openai_embedding(business_text)

    checked_ids = set()
    answers = []
    k = 5
    t = 20
    for asset_id, business_detail in db.find_similar_business_companies(
        business_embedding
    ):
        if asset_id not in checked_ids and t > 0:
            checked_ids.add(asset_id)
            print(business_detail.split("\n")[0])
            answer, reason = is_relevant_business(business_text, business_detail)
            t -= 1
            if answer:
                answers.append((asset_id, reason))
                for asset_id2, business_detail2 in db.find_similar_price_trend_companies(
                    asset_id
                )[:k]:
                    if asset_id2 not in checked_ids and t > 0:
                        checked_ids.add(asset_id2)
                        print(business_detail2.split("\n")[0])
                        answer2, reason2 = is_relevant_business(
                            business_text, business_detail2
                        )
                        t -= 1
                        if answer2:
                            answers.append((asset_id2, reason2))
            else:
                k -= 1
    return (business_text, answers)


# === Bot ===
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "주식 스크리닝 봇입니다. 관리자(@psj1026)가 서버를 켰을 때만 동작합니다."
    )


async def search_business(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.replace("/business", "").strip()
    print(query_text)
    if query_text:
        await update.message.reply_text(
            "사업보고서를 조회하고 있습니다. 잠시만 기다려주세요(30초)."
        )
        (business, companies) = find_company_by_business(update.message.text)
        if len(companies) > 0:
            betas = db.calculate_beta([asset_id for asset_id, _ in companies])
            sorted_companies = sorted(
                companies, key=lambda company: betas[company[0]], reverse=True
            )

            answer = []
            for asset_id, reason in sorted_companies:
                (_, symbol, name, _, _) = db.find_asset_by_id(asset_id)
                answer.append(
                    f"<b>{name}({symbol})</b>\n{reason}\n52주 베타: {betas[asset_id]:.2f}"
                )
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
