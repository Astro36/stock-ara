import re
import requests
from stock_ainalyst import db
from stock_ainalyst.llm import cot, openai


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
            model="gpt-3.5-turbo",
        )
        return answer
    return ""


def find_company_by_business(query):
    business_query = cot.create_business_query(query)
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
            answer, reason = cot.is_relevant_business(business_query, business_detail)
            t -= 1
            if answer:
                reason += "\n" + fetch_analysts_comment(asset_id)
                answers.append((asset_id, reason))
                for asset_id2, business_detail2 in db.find_similar_price_trend_companies(asset_id)[:k]:
                    if asset_id2 not in checked_ids and t > 0:
                        checked_ids.add(asset_id2)
                        print(business_detail2.split("\n")[0])
                        answer2, reason2 = cot.is_relevant_business(business_query, business_detail2)
                        t -= 1
                        if answer2:
                            reason2 += "\n" + fetch_analysts_comment(asset_id2)
                            answers.append((asset_id2, reason2))
            else:
                k -= 1
    return (business_query, answers)
