from dotenv import load_dotenv
import os
from stock_ainalyst import db, function, quant
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


async def reply_text(update: Update, message: str):
    print(message)
    texts = message.split("\n\n")
    text_out = ""
    for text in texts:
        if len(text_out + text + "\n\n") > 4096:
            await update.message.reply_text(text_out.strip(), parse_mode="HTML")
            text_out = text + "\n\n"
        else:
            text_out += text + "\n\n"
    await update.message.reply_text(text_out.strip(), parse_mode="HTML")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("주식 스크리닝 봇입니다. 관리자(@psj1026)가 서버를 켰을 때만 동작합니다.")


async def search_business(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text.replace("/business", "").strip()
    print(query)
    if query:
        await update.message.reply_text("사업보고서를 조회하고 있습니다. 잠시만 기다려주세요(30초).")
        (business_query, answers) = function.find_companies_by_business(query)
        if len(answers) > 0:
            # market_expected_returns = quant.market_expected_returns()
            # market_expected_returns.sort_values(ascending=False, inplace=True)

            # betas = db.calculate_beta([asset_id for asset_id, _ in companies])
            # sorted_companies = sorted(companies, key=lambda company: market_expected_returns[company[0]], reverse=True)

            answer = []
            for asset_id, reason, comment in answers:
                (_, symbol, name, _, _) = db.find_asset_by_id(asset_id)
                answer.append(
                    # f"<b>{name}({symbol})</b>\n{reason}\n52주 베타: {betas[asset_id]:.2f}, 시장기대수익률: {market_expected_returns[asset_id]*100:.2f}%"
                    f"<b>{name}({symbol})</b>\n{reason}\n<blockquote>{comment}</blockquote>"
                )
            answer = "\n\n".join(answer)
            await reply_text(update, f"<i>Query: {business_query}</i>\n\n{answer}\n\n검색결과가 만족스럽지 않다면 영어로 검색해주세요.")
        else:
            await update.message.reply_text(f"<i>Query: {business_query}</i>\n\n관련 기업이 없습니다. 검색결과가 만족스럽지 않다면 영어로 검색해주세요.", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/search 반도체 장비 회사</code>와 같이 찾으려는 기업이 영위하는 사업을 알려주세요.", parse_mode="HTML")


if __name__ == "__main__":
    load_dotenv()

    app = ApplicationBuilder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("business", search_business))
    app.run_polling()
