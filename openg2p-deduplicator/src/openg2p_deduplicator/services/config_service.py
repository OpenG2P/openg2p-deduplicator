import urllib.parse

from openg2p_fastapi_common.service import BaseService

from ..config import Settings
from ..schemas.config_request import DedupeConfig
from ..services.opensearch_service import OpenSearchClientService

_config: Settings = Settings.get_config()


class DedupeConfigService(BaseService):
    def __init__(self, **kw):
        super().__init__(**kw)

        self._opensearch_client: OpenSearchClientService = None

    @property
    def opensearch_client(self):
        if not self._opensearch_client:
            self._opensearch_client = OpenSearchClientService.get_component()
        return self._opensearch_client

    def add_or_update_config(self, config: DedupeConfig):
        self.opensearch_client.index(
            index=_config.index_name_dedupe_configs,
            id=urllib.parse.quote(config.name, safe=""),
            body=config.model_dump(),
            timeout=_config.opensearch_api_timeout,
        )

    def get_config(self, name: str) -> DedupeConfig:
        try:
            res = self.opensearch_client.get_source(
                index=_config.index_name_dedupe_configs,
                id=urllib.parse.quote(name, safe=""),
                timeout=_config.opensearch_api_timeout,
            )
            return DedupeConfig(**res)
        except Exception:
            return None

    def delete_config(self, name: str):
        self.opensearch_client.delete(
            index=_config.index_name_dedupe_configs,
            id=urllib.parse.quote(name, safe=""),
            timeout=_config.opensearch_api_timeout,
        )
