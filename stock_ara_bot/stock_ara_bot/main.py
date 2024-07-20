from dotenv import load_dotenv
load_dotenv()

import os
from stock_ara_bot import command
from stock_ara.domain.stock.calculator import StockExpectedReturnManager
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
    # user_id = update.message.from_user.id
    query = update.message.text.replace("/business", "").strip()
    print(query)
    if query:
        await update.message.reply_text("사업보고서를 조회하고 있습니다. 잠시만 기다려주세요.")
        (business_query, answers) = command.search_stock_by_business(query)
        if len(answers) > 0:
            answer = []
            for stock, reason in answers:
                answer.append(f"<b>{stock}</b>\n{reason}")
            answer = "\n\n".join(answer)
            await reply_text(update, f"<i>Query: {business_query}</i>\n\n{answer}\n\n검색결과가 만족스럽지 않다면 영어로 검색해주세요.")
        else:
            await update.message.reply_text(f"<i>Query: {business_query}</i>\n\n관련 기업이 없습니다. 검색결과가 만족스럽지 않다면 영어로 검색해주세요.", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/business 반도체 장비 회사</code>와 같이 찾으려는 기업이 영위하는 사업을 알려주세요.", parse_mode="HTML")


async def search_stock_by_keyword(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # user_id = update.message.from_user.id
    keyword = update.message.text.replace("/keyword", "").strip()

    if keyword and len(keyword) >= 2:
        answers = command.search_stock_by_keyword(keyword)
        if len(answers) > 0:
            answer = []
            for stock, wheres in answers:
                answer.append(f"<b>{stock}</b>\n" + "\n".join([f"[{i+1}] {where.replace(keyword, f'<u>{keyword}</u>') }" for i, where in enumerate(wheres)]))
            answer = "\n\n".join(answer)
            await reply_text(update, f"<i>Keyword: {keyword}</i>\n\n{answer}")
        else:
            await update.message.reply_text(f"검색결과가 존재하지 않습니다.", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/keyword 웨이퍼</code>와 같이 사업보고서에서 찾으려는 단어를 알려주세요.", parse_mode="HTML")


async def research_stock(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # user_id = update.message.from_user.id
    stock_name = update.message.text.replace("/stock", "").strip()

    if stock_name:
        await update.message.reply_text("애널리스트 리포트를 조회하고 있습니다. 잠시만 기다려주세요.")
        try:
            (stock, business_summary, comment, capm_expected_return, implied_expected_return) = command.research_stock(stock_name)
            await reply_text(
                update,
                f"<b>{stock}</b>\n{business_summary}\n<blockquote><b>애널리스트 코멘트</b>\n{comment}</blockquote>\nCAPM 기대수익률: {capm_expected_return*100:.2f}%, 내재 기대수익률: {implied_expected_return*100:.2f}%",
            )
        except:
            await update.message.reply_text("종목을 찾을 수 없습니다.", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/stock 삼성전자</code>와 같이 분석하려는 종목을 알려주세요.", parse_mode="HTML")


async def make_optimal_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # user_id = update.message.from_user.id
    stock_names = list(map(lambda x: x.strip(), update.message.text.replace("/portfolio", "").split(",")))

    if len(stock_names) >= 2:
        try:
            portfolio_weights = command.make_optimal_portfolio(stock_names)
            answer = []
            for stock, weight, expected_return, volatility in sorted(portfolio_weights, key=lambda x: x[1], reverse=True):
                answer.append(f"<b>{stock}</b>\n비중: {f'{weight*100:.2f}%' if weight >= 0.001 else '0% (편입하지 않음)'}\n내재 기대수익률: {expected_return*100:.2f}%, 52주 변동성: {volatility:.2f}%")
            answer = "\n\n".join(answer)
            await reply_text(update, f"{answer}\n\n평균-분산 모형으로 최적화된 포트폴리오입니다. 종목간 상관계수에 따라 편입되지 않는 종목이 발생할 수 있습니다.")
        except:
            await update.message.reply_text("일부 종목을 찾을 수 없습니다.", parse_mode="HTML")
    else:
        await update.message.reply_text("<code>/portfolio 삼성전자,SK하이닉스,한미반도체</code>와 같이 포트폴리오를 구성하는 종목을 알려주세요.", parse_mode="HTML")


if __name__ == "__main__":
    StockExpectedReturnManager().update()
    print("Prepared expected returns for all stocks")

    app = ApplicationBuilder().token(os.getenv("TELEGRAM_BOT_TOKEN")).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("business", search_stock_by_business))  # 종목명, 선정이유
    app.add_handler(CommandHandler("keyword", search_stock_by_keyword))  # 종목명, 선정이유
    app.add_handler(CommandHandler("stock", research_stock))  # 종목명, 애널리스트 코멘트, 기대수익률
    app.add_handler(CommandHandler("portfolio", make_optimal_portfolio))  # 기대수익률, 포트폴리오
    app.run_polling(allowed_updates=Update.ALL_TYPES)
