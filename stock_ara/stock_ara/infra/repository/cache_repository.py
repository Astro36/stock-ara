from redis import Redis


class CacheRepository:
    def __init__(self, client: Redis) -> None:
        self.client = client

    def get_capm_expected_return(self, asset_id: int) -> float:
        return float(self.client.get(f"capm_expected_return:asset_id#{asset_id}"))

    def set_capm_expected_return(self, asset_id: int, expected_return: float):
        self.client.set(f"capm_expected_return:asset_id#{asset_id}", expected_return)

    def get_implied_expected_return(self, asset_id: int) -> float:
        return float(self.client.get(f"implied_expected_return:asset_id#{asset_id}"))

    def set_implied_expected_return(self, asset_id: int, expected_return: float):
        self.client.set(f"implied_expected_return:asset_id#{asset_id}", expected_return)
