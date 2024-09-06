from typing import Any, Sequence, List, Union, Dict
from sensor_tower.operators.base import SensorTowerBaseOperator
from .top_creatives import CreativesTopsEndpoint

class AdIntelligenceEndpointFactory:

    def __init__(self, ds, http_conn_id):
        self.ds = ds
        self._http_conn_id = http_conn_id

    @property
    def creative_tops_endpoint(self):
        return CreativesTopsEndpoint    
    
                    
    
    def creative_top_filter(
            self
        ):
        return CreativesTopsEndpoint(self.ds, self._http_conn_id) \
                    .filter
                
    
class AdIntelligenceOperator(SensorTowerBaseOperator):

    template_fields: Sequence[str] = (
        "bucket",
        "gcs_prefix",
        "ds",
    )

    def __init__(
            self,
            task_id: str,
            endpoint_class,
            endpoint_filter,
            ds,
            http_conn_id,
            gcs_bucket: str, 
            gcs_prefix: str,
            **kwargs
        ):

        super().__init__(
            ds=ds,
            task_id=task_id, 
            gcs_bucket=gcs_bucket,
            gcs_prefix=gcs_prefix,
            **kwargs
        )

        self._http_conn_id = http_conn_id
        self._endpoint_class = endpoint_class    
        self._endpoint_filter = endpoint_filter
    
    def execute(self, context: Any):
        if not self._endpoint_class:
            raise ValueError(f'Invalid Endpoint: {self._endpoint}')
        
        data = self._endpoint_class(self.ds, self._http_conn_id).fetch_data()
        self._upload_data(data)