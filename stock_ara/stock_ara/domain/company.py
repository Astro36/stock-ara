from dataclasses import dataclass


@dataclass(frozen=True)
class Company:
    id: int
    name: str
    business_summary: str
    business_raw: str
    business_embedding: list[float]
    stock_id: int
