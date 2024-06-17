from pgvector.psycopg import register_vector
import psycopg
import os
from stock_ara.models.asset import StockRepository
from stock_ara.models.asset_price import AssetPriceRepository
from stock_ara.models.company import CompanyRepository


conn = psycopg.connect(f"host={os.getenv('POSTGRES_HOST', 'localhost')} dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}")
register_vector(conn)

stock_repository = StockRepository(conn)
asset_price_repository = AssetPriceRepository(conn)
company_repository = CompanyRepository(conn)
