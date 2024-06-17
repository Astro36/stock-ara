from stock_ara.datasource import naver
from stock_ara.function import prompt
from stock_ara.llm import openai
from stock_ara.db import company_repository


def is_relevant_business(asset_id: int, business_query: str) -> tuple[bool, str]:
    if (company := company_repository.find_by_stock_id(asset_id)) is not None:
        business_summary = prompt.create_business_summary_from_query(business_query, company.business_raw)
        answer = openai.request_gpt_answer(
            [
                'You are a corporate analyst. Read the given business report summary and answer "True" or "False" whether the business this company does is relevant to the given query.',
                f'Business report summary: """{business_summary}"""\n\nQuery: {business_query}',
            ],
            model="gpt-4o",
        )
        print(business_summary, answer)
        if "true" in answer.lower():
            return (True, business_summary)
    return (False, None)


def summarize_company_business(business_raw: str) -> str:
    if business_raw is not None:
        business_summary = openai.request_gpt_answer(
            [
                "Read the given business report excerpt and summarize it in an English sentence of 300 characters or less, focusing on the company's specific business details that can distinguish it from other companies, such as product name and sales proportion. Don't write about market and company forecasts.",
                f'Business report: """{business_raw[:2000]}"""',
            ],
            model="gpt-4o",
        )
        return business_summary
    return None


def summarize_analysts_comments(stock_symbol: str) -> str:
    comments = naver.fetch_analysts_comments(stock_symbol)
    if len(comments) >= 1:
        answer = openai.request_gpt_answer(
            [
                "Based on the given content, summarize the company's positive and negative outlook in in English sentences 400 characters or less. Do not output results in list format.",
                "\n\n".join(comments)[:1000],
            ],
        )
        return answer
    return "애널리스트 의견없음"
