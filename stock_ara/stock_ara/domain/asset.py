from dataclasses import dataclass


@dataclass(frozen=True)
class Asset:
    id: int
    name: str
    symbol: str
    exchange: str
    currency: str

    def __str__(self) -> str:
        return f"{self.name}({self.symbol})"
