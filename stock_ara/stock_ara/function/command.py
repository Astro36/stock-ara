import cvxpy as cp
import redis
import math
import multiprocessing as mp
import pandas as pd
import re
from stock_ara import db
from stock_ara.function import prompt, rag
from stock_ara.llm import openai, papago
from stock_ara.db import stock_repository, asset_price_repository, company_repository


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
            for company in company_repository.find_all_by_business(business_embedding, offset=search_offset):
                q.put(company.stock_id)
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
                        answers.append((stock_repository.find_by_id(asset_id), papago.translate(reason)))

                        for stock2 in stock_repository.find_all_by_correlation(asset_id):
                            if stock2.id not in checked_asset_ids:
                                q.put(stock2.id)

            if len(answers) >= 5 or len(checked_asset_ids) >= 25:
                break

    return (business_query, answers)


def search_stock_by_keyword(keyword: str):
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


def analyze_stock(stock_name):
    stock = stock_repository.find_by_name(stock_name)
    company = company_repository.find_by_stock_id(stock.id)
    business_summary = papago.translate(rag.summarize_company_business(company.business_raw))
    comment = papago.translate(rag.summarize_analysts_comments(stock.symbol))

    capm_expected_return = float(r.get(f"capm_expected_return:asset_id#{stock.id}"))
    implied_expected_return = float(r.get(f"implied_expected_return:asset_id#{stock.id}"))

    return (stock, business_summary, comment, capm_expected_return, implied_expected_return)


def make_portfolio(stock_names):
    stocks = [stock_repository.find_by_name(name) for name in stock_names]
    asset_prices = pd.concat([asset_price_repository.get_asset_prices(stock.id) for stock in stocks], axis=1)
    asset_returns = asset_prices.pct_change(fill_method=None).dropna(how="all")
    asset_covs = asset_returns.cov().fillna(0) * 52
    asset_expected_returns = pd.Series([float(r.get(f"implied_expected_return:asset_id#{stock.id}")) for stock in stocks], index=[stock.id for stock in stocks], dtype=float)
    asset_expected_returns = asset_expected_returns.to_numpy()

    portfolio_weights = cp.Variable(len(stocks))
    portfolio_return = asset_expected_returns.T @ portfolio_weights
    portfolio_risk = cp.quad_form(portfolio_weights, asset_covs)
    prob = cp.Problem(cp.Maximize(portfolio_return - portfolio_risk), [cp.sum(portfolio_weights) == 1, portfolio_weights >= 0])
    prob.solve()

    portfolio_weights = [(stocks[idx], weight, asset_expected_returns[idx], math.sqrt(asset_covs.iloc[idx, idx])) for idx, weight in enumerate(portfolio_weights.value)]
    return portfolio_weights
