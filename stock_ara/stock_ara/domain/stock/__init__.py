from dataclasses import dataclass
from stock_ara.domain.asset import Asset


@dataclass(frozen=True)
class Stock(Asset):
    company_id: int
