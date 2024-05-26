import re
import requests
from stock_ainalyst import db
from stock_ainalyst.llm import openai, papago


def create_business_query(query):
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


def is_relevant_business(business_query, business_raw):
    business_summary = openai.request_gpt_answer(
        [
            "You are a corporate analyst. Read the given business report, extract the parts relevant to the given query in English. Write only one sentence.",
            f'Business report: """{business_raw[:2000]}"""\n\nQuery: {business_query}',
        ],
        model="gpt-4o",
    )
    print(business_summary)
    answer = openai.request_gpt_answer(
        [
            'You are a corporate analyst. Read the given business report summary and answer "True" or "False" whether the business this company does is relevant to the given query.',
            f'Business report summary: """{business_summary}"""\n\nQuery: {business_query}',
        ],
        model="gpt-4o",
    )
    print(answer)
    if "true" in answer.lower():
        business_summary_ko = papago.translate(business_summary)
        return (True, business_summary_ko)
    return (False, None)


def fetch_analysts_comment(asset_id):
    (_, symbol, _, _, _) = db.find_asset_by_id(asset_id)
    symbol = symbol.split(".")[0]

    r = requests.get(f"https://finance.naver.com/research/company_list.naver?searchType=itemCode&itemCode={symbol}")

    contents = []
    for nid in re.findall(f"company_read.naver\?nid=(\d+)&page=1&searchType=itemCode&itemCode={symbol}", r.text)[:5]:
        r = requests.get(
            f"https://finance.naver.com/research/company_read.naver?nid={nid}&page=1&searchType=itemCode&itemCode={symbol}"
        )
        text = r.text.split('class="view_cnt">')[1].split("</td>")[0].strip()
        text = re.sub("</?[^>]*>", "", text)
        text = re.sub("[\w]+\.pdf", "", text)
        text = re.sub("\s+", " ", text)
        contents.append(text)

    if len(contents) >= 1:
        answer = openai.request_gpt_answer(
            [
                "Summarize information about the current status of the company in 200 characters or less.",
                "\n\n".join(contents)[:1000],
            ],
        )
        answer_ko = papago.translate(answer)
        return answer_ko
    return ""


def find_company_by_business(query):
    business_query = create_business_query(query)
    business_embedding = openai.request_embedding(business_query)
    print("business_query", business_query)

    checked_ids = set()
    answers = []
    k = 4
    t = 16
    for asset_id, business_detail in db.find_similar_business_companies(business_embedding):
        if asset_id not in checked_ids and t > 0:
            checked_ids.add(asset_id)
            print(business_detail.split("\n")[0])
            answer, reason = is_relevant_business(business_query, business_detail)
            t -= 1
            if answer:
                reason += "\n" + fetch_analysts_comment(asset_id)
                answers.append((asset_id, reason))
                for asset_id2, business_detail2 in db.find_similar_price_trend_companies(asset_id)[:k]:
                    if asset_id2 not in checked_ids and t > 0:
                        checked_ids.add(asset_id2)
                        print(business_detail2.split("\n")[0])
                        answer2, reason2 = is_relevant_business(business_query, business_detail2)
                        t -= 1
                        if answer2:
                            reason2 += "\n" + fetch_analysts_comment(asset_id2)
                            answers.append((asset_id2, reason2))
            else:
                k -= 1
    return (business_query, answers)
