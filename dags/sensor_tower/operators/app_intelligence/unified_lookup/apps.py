from typing import Dict, List
from sensor_tower.operators.endpoint import TensorTowerAbstractEndpoint
from dataclasses import dataclass



class UnifiedAppsEndpoint(TensorTowerAbstractEndpoint):

    def __init__(
        self,
        ds: str,
        http_conn_id: str
    ):
        super().__init__(http_conn_id = http_conn_id)
        self.ds = ds

    def _format_params(self) -> Dict:
        return super()._format_params()
    
    def _format_response_result(self, results: List) -> List:
        return super()._format_response_result(results)
    
    def fetch_data(self):
        return
    
