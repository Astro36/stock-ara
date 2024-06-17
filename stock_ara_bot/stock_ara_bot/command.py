from stock_ara.domain.stock.analyst import StockAnalyst
from stock_ara.domain.stock.screener import StockScreener
from stock_ara.domain.portfolio_manager import PortfolioManager
from stock_ara.infra.database import stock_repository


def search_stock_by_business(query: str) -> tuple[str, list[tuple[str, str]]]:
    stock_screener = StockScreener()
    result = stock_screener.search_by_business(query)
    return result


def search_stock_by_keyword(keyword: str):
    stock_screener = StockScreener()
    result = stock_screener.search_by_keyword(keyword)
    return result


def research_stock(stock_name):
    stock = stock_repository.find_by_name(stock_name)
    stock_analyst = StockAnalyst()
    result = stock_analyst.research(stock)
    return (stock, result[0], result[1], result[2], result[3])


def make_optimal_portfolio(stock_names):
    stocks = [stock_repository.find_by_name(name) for name in stock_names]
    manager = PortfolioManager()
    portfolio = manager.make_optimal_portfolio(stocks)
    return portfolio
