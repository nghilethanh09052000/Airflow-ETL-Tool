from typing import List, Dict, Union
from abc import abstractmethod
from sensor_tower.hooks.sensor_tower_api import SensorTowerApiHook
from sensor_tower.scripts.utils import (
    MobileOperatingSystem
)


class SensorTowerAbstractEndpoint:

    def __init__(self, http_conn_id: str):
        
        self.api = SensorTowerApiHook(http_conn_id = http_conn_id)

    @abstractmethod
    def _format_params(self) -> Dict:
        pass

    @abstractmethod
    def _format_response_result(self, results: List) -> List:
        pass

    @abstractmethod
    def fetch_data(self):
        pass
    
