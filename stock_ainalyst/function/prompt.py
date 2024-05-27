from stock_ainalyst.llm import openai


def create_business_query(query: str) -> str:
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


def create_business_summary_from_query(business_query: str, business_raw: str) -> str:
    business_summary = openai.request_gpt_answer(
        [
            "Read the given business report, extract the parts relevant to the given query in English. Write only one sentence.",
            f'Business report: """{business_raw[:2000]}"""\n\nQuery: {business_query}',
        ],
    )
    return business_summary
