from pgvector.psycopg import register_vector
import psycopg
import os
import redis
from stock_ara.infra.repository.asset_price_repository import AssetPriceRepository
from stock_ara.infra.repository.company_repository import CompanyRepository
from stock_ara.infra.repository.stock_repository import StockRepository
from stock_ara.infra.repository.cache_repository import CacheRepository


conn = psycopg.connect(f"host={os.getenv('POSTGRES_HOST', 'localhost')} dbname={os.getenv('POSTGRES_DB')} user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}")
register_vector(conn)

stock_repository = StockRepository(conn)
asset_price_repository = AssetPriceRepository(conn)
company_repository = CompanyRepository(conn)

r = redis.Redis(host=os.getenv('REDIS_HOST', 'localhost'), port=6379, db=0, decode_responses=True, protocol=3)

cache_repository = CacheRepository(r)
