from openg2p_fastapi_common.service import BaseService

from ..schemas.config_request import DedupeConfig
from ..services.opensearch_service import OpenSearchClientService


class DedupeConfigService(BaseService):
    def __init__(self, **kw):
        super().__init__(**kw)

        self._opensearch_client_service = None

    @property
    def opensearch_client_service(self):
        if not self._opensearch_client_service:
            self._opensearch_client_service = OpenSearchClientService.get_component()
        return self._opensearch_client_service

    async def add_or_update_config(self, config: DedupeConfig):
        pass

    async def get_config(self, name: str) -> DedupeConfig:
        pass

    async def delete_config(self, name: str):
        pass
