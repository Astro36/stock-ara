import multiprocessing as mp
import re
from stock_ara.domain.stock import Stock
from stock_ara.infra.database import company_repository, stock_repository
from stock_ara.infra.llm import openai, papago


class StockScreener:
    def search_by_business(self, query: str) -> tuple[str, list[tuple[Stock, str]]]:
        business_query = self._create_business_query(query)
        print("business_query", business_query)
        business_embedding = openai.request_embedding(business_query)

        checked_stock_ids = set()
        answers = []

        with mp.Pool(10) as p:
            q = mp.Queue()
            search_offset = 0

            while True:
                for company in company_repository.find_all_by_business(business_embedding, offset=search_offset):
                    q.put(company.stock_id)
                search_offset += 5

                if not q.empty():
                    x = []
                    for _ in range(10):
                        if not q.empty():
                            x.append((q.get(), business_query))
                    result = p.map(_is_relevant_business_wrapper, x)
                    checked_stock_ids.update([stock_id for stock_id, _ in x])

                    for idx, (relevant, reason) in enumerate(result):
                        if relevant:
                            stock_id, _ = x[idx]
                            answers.append((stock_repository.find_by_id(stock_id), papago.translate(reason)))

                            for stock2 in stock_repository.find_all_by_correlation(stock_id):
                                if stock2.id not in checked_stock_ids:
                                    q.put(stock2.id)

                if len(answers) >= 5 or len(checked_stock_ids) >= 25:
                    break

        return (business_query, answers)

    def search_by_keyword(self, keyword: str) -> list[Stock, list[str]]:
        companies = company_repository.find_all_by_keyword(keyword)
        answers = []
        for company in companies:
            wheres = []
            for m in re.finditer(keyword, company.business_raw):
                start_idx = max(0, m.start() - 25)
                end_idx = min(len(company.business_raw), m.end() + 25)
                content = company.business_raw[start_idx:end_idx]
                wheres.append(content.replace("\n", " "))
                if len(wheres) >= 5:
                    break
            answers.append((stock_repository.find_by_id(company.stock_id), wheres))
        return answers

    def _create_business_query(self, query: str) -> str:
        business_query = openai.request_gpt_answer(
            [
                "Translate the given query into an English sentence about what this company does in detailed, as in the example.",
                # Few-shot prompting
                "무기 만드는 회사",
                "The company manufactures defensive weapons.",
                "반도체와 관련된 회사",
                "This company is related to semiconductors.",
                "원자력 발전소를 건설하는 회사",
                "The company builds nuclear power plants.",
                "이차전지 양극재를 만드는 회사",
                "This company manufactures secondary battery cathode materials.",
                "면역항암제",
                "This company's main products are cancer immunotherapies.",
                "물류",
                "This company is logistics company.",
                "게임",
                "This company's main products are games.",
                query,
            ],
        )
        business_keyword = openai.request_gpt_answer(
            [
                "What is the noun keyword of the given sentence about the business of a company.",
                # Few-shot prompting
                "The company manufactures equipment used for the back-end processes of semiconductor production.",
                "Semiconductor back-end process.",
                "This company is involved in the cosmetics industry.",
                "Cosmetics",
                business_query,
            ],
        )
        business_description = openai.request_gpt_answer(
            [
                "Describe the given keyword in 100 characters or less.",
                # Few-shot prompting
                "Semiconductor back-end process.",
                "The semiconductor back-end process involves assembly and testing of chips after wafer fabrication.",
                business_keyword,
            ],
        )
        business_query += " " + business_description
        return business_query


def _is_relevant_business_wrapper(x):
    stock_id, business_query = x
    stock = stock_repository.find_by_id(stock_id)

    def _summarize_company_business(business_raw: str, business_query: str) -> str:
        business_summary = openai.request_gpt_answer(
            [
                "Read the given business report, extract the parts relevant to the given query in English. Write only one sentence.",
                f'Business report: """{business_raw[:2000]}"""\n\nQuery: {business_query}',
            ],
        )
        return business_summary

    def _is_relevant_business(stock: Stock, business_query: str) -> tuple[bool, str]:
        if (company := company_repository.find_by_stock_id(stock.id)) is not None:
            business_summary = _summarize_company_business(company.business_raw, business_query)
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

    return _is_relevant_business(stock, business_query)
