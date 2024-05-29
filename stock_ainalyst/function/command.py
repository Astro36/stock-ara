import redis
import multiprocessing as mp
from queue import Queue
from stock_ainalyst import db
from stock_ainalyst.function import prompt, rag
from stock_ainalyst.llm import openai, papago


r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True, protocol=3)


def fn(x):
    asset_id, bq = x
    return rag.is_relevant_business(asset_id, bq)


def find_companies_by_business(query: str) -> tuple[str, list[tuple[str, str]]]:
    business_query = prompt.create_business_query(query)
    print("business_query", business_query)
    business_embedding = openai.request_embedding(business_query)

    checked_asset_ids = set()
    answers = []

    with mp.Pool(10) as p:
        q = mp.Queue()
        for asset_id in db.find_asset_ids_by_business(business_embedding):
            q.put(asset_id)

        while not q.empty() or len(answers) >= 5 or len(checked_asset_ids) >= 25:
            x = []
            for _ in range(10):
                if not q.empty():
                    x.append((q.get(), business_query))
            result = p.map(fn, x)
            checked_asset_ids.update([asset_id for asset_id, _ in x])

            for idx, (relevant, reason) in enumerate(result):
                if relevant:
                    answers.append((asset_id, papago.translate(reason)))
                    asset_id, _ = x[idx]

                    for asset_id2 in db.find_asset_ids_by_price_change_correlation(asset_id):
                        if asset_id2 not in checked_asset_ids:
                            q.put(asset_id2)

    return (business_query, answers)


def analyze_stock(stock_name):
    (_, _, asset_id, _, _, _) = db.find_company_by_name(stock_name)
    business_summary = papago.translate(rag.summarize_company_business(asset_id))
    comment = papago.translate(rag.summarize_analysts_comments(asset_id))

    capm_expected_return = float(r.get(f"capm_expected_return:asset_id#{asset_id}"))
    implied_expected_return = float(r.get(f"implied_expected_return:asset_id#{asset_id}"))
    (_, symbol, _, _, _) = db.find_asset_by_id(asset_id)

    return (symbol, stock_name, business_summary, comment, capm_expected_return, implied_expected_return)
