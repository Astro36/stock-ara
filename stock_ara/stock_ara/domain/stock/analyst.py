import re
import requests
from stock_ara.domain.stock import Stock
from stock_ara.infra.database import cache_repository, company_repository
from stock_ara.infra.llm import openai, papago


class StockAnalyst:
    def ask_business_forecast(self, stock: Stock) -> str:
        analyst_comments = self._fetch_naver_analyst_comments(stock)
        summarized_comment = papago.translate(self._summarize_analyst_comments(analyst_comments))
        return summarized_comment

    def research(self, stock: Stock) -> tuple[str, str, float, float]:
        company = company_repository.find_by_stock_id(stock.id)
        business_summary = papago.translate(company.business_summary)
        business_forecast = self.ask_business_forecast(stock)
        return (
            business_summary,
            business_forecast,
            cache_repository.get_capm_expected_return(stock.id),
            cache_repository.get_implied_expected_return(stock.id),
        )

    def _fetch_naver_analyst_comments(self, stock: Stock) -> list[str]:
        symbol = stock.symbol.split(".")[0]
        r = requests.get(f"https://finance.naver.com/research/company_list.naver?searchType=itemCode&itemCode={symbol}")
        comments = []
        for nid in re.findall(rf"company_read.naver\?nid=(\d+)&page=1&searchType=itemCode&itemCode={symbol}", r.text)[:5]:
            r = requests.get(f"https://finance.naver.com/research/company_read.naver?nid={nid}&page=1&searchType=itemCode&itemCode={symbol}")
            text = r.text.split('class="view_cnt">')[1].split("</td>")[0].strip()
            text = re.sub(r"</?[^>]*>", "", text)
            text = re.sub(r"[\w]+\.pdf", "", text)
            text = re.sub(r"\s+", " ", text)
            text = text.strip()
            comments.append(text)
        return comments

    def _summarize_analyst_comments(self, comments: list[str]) -> str:
        if len(comments) >= 1:
            answer = openai.request_gpt_answer(
                [
                    "Based on the given content, summarize the company's positive and negative outlook in in English sentences 400 characters or less. Do not output results in list format.",
                    "\n\n".join(comments)[:1000],
                ],
            )
            return answer
        return "애널리스트 의견없음"
