from dotenv import load_dotenv
import os
from stock_ainalyst import db
from stock_ainalyst.function import command
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes


async def reply_text(update: Update, message: str, reply_markup=None):
    texts = message.split("\n\n")
    text_out = ""
    for text in texts:
        if len(text_out + text + "\n\n") > 4096:
            await update.message.reply_text(text_out.strip(), parse_mode="HTML")
            text_out = text + "\n\n"
        else:
            text_out += text + "\n\n"
    await update.message.reply_text(text_out.strip(), parse_mode="HTML", reply_markup=reply_markup)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("주식 스크리닝 봇입니다. 관리자(@psj1026)가 서버를 켰을 때만 동작합니다.")


async def search_stock_by_business(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    query = update.message.text.replace("/business", "").strip()
    print(query)
    if query:
        await update.message.reply_text("사업보고서를 조회하고 있습니다. 잠시만 기다려주세요.")
        (business_query, answers) = command.find_companies_by_business(query)
        if len(answers) > 0:
            answer = []
            for asset_id, reason in answers:
                (_, symbol, name, _, _) = db.find_asset_by_id(asset_id)
                answer.append(
                    f"<b>{name}({symbol})</b>\n{reason}"
                )
            answer = "\n\n".join(answer)
            await reply_text(update, f"<i>Query: {business_query}</i>\n\n{answer}\n\n검색결과가 만족스럽지 않다면 영어로 검색해주세요.")
        else:
            await update.message.reply_text(f"<i>Query: {business_query}</i>\n\n관련 기업이 없습니다. 검색결과가 만족스럽지 않다면 영어로 검색해주세요.", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/business 반도체 장비 회사</code>와 같이 찾으려는 기업이 영위하는 사업을 알려주세요.", parse_mode="HTML")


# async def search_stock_by_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
#     user_id = update.message.from_user.id
#     keyword = update.message.text.replace("/keyword", "").strip()

#     if keyword:
#         pass
#     else:
#         await update.message.reply_text("<code>/keyword 반도체</code>와 같이 사업보고서에서 찾으려는 단어를 알려주세요.", parse_mode="HTML")


async def analyze_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    stock_name = update.message.text.replace("/analyze", "").strip()

    if stock_name:
        await update.message.reply_text("애널리스트 리포트를 조회하고 있습니다. 잠시만 기다려주세요.")
        try:
            (symbol, stock_name, business_summary, comment, capm_expected_return, implied_expected_return) = command.analyze_stock(stock_name)
            await reply_text(update, f"<b>{stock_name}({symbol})</b>\n{business_summary}\n<blockquote>{comment}</blockquote>\nCAPM 기대수익률: {capm_expected_return*100:.2f}%, 내재 기대수익률: {implied_expected_return*100:.2f}%")
        except:
            await update.message.reply_text("종목을 찾을 수 없습니다.", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/analyze 삼성전자</code>와 같이 분석하려는 종목을 알려주세요.", parse_mode="HTML")


if __name__ == "__main__":
    load_dotenv()

    app = ApplicationBuilder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("business", search_stock_by_business))  # 종목명, 선정이유
    # app.add_handler(CommandHandler("keyword", search_stock_by_keyword)) # 종목명, 선정이유
    app.add_handler(CommandHandler("analyze", analyze_stock)) # 종목명, 애널리스트 코멘트, 기대수익률
    # app.add_handler(CommandHandler("portfolio", make_portfolio)) # 기대수익률, 포트폴리오
    app.run_polling(allowed_updates=Update.ALL_TYPES)
