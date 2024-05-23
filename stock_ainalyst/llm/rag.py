from stock_ainalyst import db
from stock_ainalyst.llm import cot, openai


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
                answers.append((asset_id, reason))
                for asset_id2, business_detail2 in db.find_similar_price_trend_companies(asset_id)[:k]:
                    if asset_id2 not in checked_ids and t > 0:
                        checked_ids.add(asset_id2)
                        print(business_detail2.split("\n")[0])
                        answer2, reason2 = cot.is_relevant_business(business_query, business_detail2)
                        t -= 1
                        if answer2:
                            answers.append((asset_id2, reason2))
            else:
                k -= 1
    return (business_query, answers)
