import cvxpy as cp
import redis
import math
import multiprocessing as mp
import pandas as pd
import re
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
        search_offset = 0

        while True:
            for stock in db.find_stocks_by_business(business_embedding, offset=search_offset):
                asset_id = stock[0]
                q.put(asset_id)
            search_offset += 5

            if not q.empty():
                x = []
                for _ in range(10):
                    if not q.empty():
                        x.append((q.get(), business_query))
                result = p.map(fn, x)
                checked_asset_ids.update([asset_id for asset_id, _ in x])

                for idx, (relevant, reason) in enumerate(result):
                    if relevant:
                        asset_id, _ = x[idx]
                        answers.append((asset_id, papago.translate(reason)))

                        for stock2 in db.find_stocks_by_weekly_return_correlation(asset_id):
                            asset_id2 = stock2[0]
                            if asset_id2 not in checked_asset_ids:
                                q.put(asset_id2)

            if len(answers) >= 5 or len(checked_asset_ids) >= 25:
                break

    return (business_query, answers)


def search_stock_by_keyword(keyword: str):
    stocks = db.find_stocks_by_keyword(keyword)

    answers = []
    for stock in stocks:
        asset_id = stock[0]
        business_raw = stock[7]
        wheres = []
        for m in re.finditer(keyword, business_raw):
            start_idx = max(0, m.start() - 25)
            end_idx = min(len(business_raw), m.end() + 25)
            content = business_raw[start_idx:end_idx]
            if start_idx > 0:
                content = "…" + content
            if end_idx < len(business_raw) - 1:
                content += "…"
            wheres.append(content)
            if len(wheres) >= 5:
                break
        answers.append((asset_id, wheres))
    return answers


def analyze_stock(stock_name):
    stock = db.find_stock_by_name(stock_name)
    business_summary = papago.translate(rag.summarize_company_business(stock))
    comment = papago.translate(rag.summarize_analysts_comments(stock))

    asset_id = stock[0]
    capm_expected_return = float(r.get(f"capm_expected_return:asset_id#{asset_id}"))
    implied_expected_return = float(r.get(f"implied_expected_return:asset_id#{asset_id}"))

    symbol = stock[2]
    return (symbol, stock_name, business_summary, comment, capm_expected_return, implied_expected_return)


def make_portfolio(stock_names):
    asset_ids = [db.find_stock_by_name(name)[0] for name in stock_names]
    asset_prices = pd.concat([db.find_weekly_close_prices_by_id(asset_id) for asset_id in asset_ids], axis=1)
    asset_returns = asset_prices.pct_change(fill_method=None).dropna(how="all")
    asset_covs = asset_returns.cov().fillna(0) * 52
    asset_expected_returns = pd.Series([float(r.get(f"implied_expected_return:asset_id#{asset_id}")) for asset_id in asset_ids], index=asset_ids, dtype=float)
    asset_expected_returns = asset_expected_returns.to_numpy()

    portfolio_weights = cp.Variable(len(asset_ids))
    portfolio_return = asset_expected_returns.T @ portfolio_weights
    portfolio_risk = cp.quad_form(portfolio_weights, asset_covs)
    prob = cp.Problem(cp.Maximize(portfolio_return - portfolio_risk), [cp.sum(portfolio_weights) == 1, portfolio_weights >= 0])
    prob.solve()

    portfolio_weights = [(asset_ids[idx], weight, asset_expected_returns[idx], math.sqrt(asset_covs.iloc[idx, idx])) for idx, weight in enumerate(portfolio_weights.value)]
    return portfolio_weights
