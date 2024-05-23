from dotenv import load_dotenv
import os
from stock_ainalyst import db
from stock_ainalyst.llm import rag
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("주식 스크리닝 봇입니다. 관리자(@psj1026)가 서버를 켰을 때만 동작합니다.")


async def search_business(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_text = update.message.text.replace("/business", "").strip()
    print(query_text)
    if query_text:
        await update.message.reply_text("사업보고서를 조회하고 있습니다. 잠시만 기다려주세요(30초).")
        (business, companies) = rag.find_company_by_business(update.message.text)
        if len(companies) > 0:
            betas = db.calculate_beta([asset_id for asset_id, _ in companies])
            sorted_companies = sorted(companies, key=lambda company: betas[company[0]], reverse=True)

            answer = []
            for asset_id, reason in sorted_companies:
                (_, symbol, name, _, _) = db.find_asset_by_id(asset_id)
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
