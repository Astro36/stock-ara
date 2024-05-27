from multiprocessing import Pool
from stock_ainalyst import db
from stock_ainalyst.function import prompt, rag
from stock_ainalyst.llm import openai, papago


def fn(x):
    asset_id, bq = x
    return rag.is_relevant_business(asset_id, bq)


def find_companies_by_business(query: str) -> tuple[str, list[tuple[str, str]]]:
    business_query = prompt.create_business_query(query)
    print("business_query", business_query)
    business_embedding = openai.request_embedding(business_query)

    checked_asset_ids = set()
    answers = []

    with Pool(10) as p:
        while True:
            if len(answers) >= 5 or len(checked_asset_ids) >= 20:
                break

            similar_business_company_asset_ids = db.find_asset_ids_by_business(business_embedding)
            similar_business_company_asset_ids = list(filter(lambda asset_id: asset_id not in checked_asset_ids, similar_business_company_asset_ids))
            checked_asset_ids.update(similar_business_company_asset_ids)
            print(similar_business_company_asset_ids)

            result = p.map(fn, [(id, business_query) for id in similar_business_company_asset_ids])
            for idx, (relevant, reason) in enumerate(result):
                if relevant:
                    asset_id = similar_business_company_asset_ids[idx]
                    comment = rag.summarize_analysts_comments(asset_id)
                    answers.append((asset_id, papago.translate(reason), papago.translate(comment)))

            if len(answers) >= 5 or len(checked_asset_ids) >= 20:
                break

            for idx, (relevant, reason) in enumerate(result):
                if relevant:
                    if len(answers) >= 5 or len(checked_asset_ids) >= 20:
                        break

                    asset_id = similar_business_company_asset_ids[idx]
                    similar_price_trend_company_asset_ids = db.find_asset_ids_by_price_change_correlation(asset_id)
                    similar_price_trend_company_asset_ids = list(filter(lambda asset_id: asset_id not in checked_asset_ids, similar_price_trend_company_asset_ids))
                    checked_asset_ids.update(similar_price_trend_company_asset_ids)
                    print(similar_price_trend_company_asset_ids)

                    result2 = p.map(fn, [(id, business_query) for id in similar_price_trend_company_asset_ids])
                    for idx2, (relevant2, reason2) in enumerate(result2):
                        if relevant2:
                            asset_id2 = similar_price_trend_company_asset_ids[idx2]
                            comment2 = rag.summarize_analysts_comments(asset_id2)
                            answers.append((asset_id2, papago.translate(reason2), papago.translate(comment2)))

    return (business_query, answers)
