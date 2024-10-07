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

    async def add_or_update_config(self, config: DedupeConfig):
        await self.opensearch_client.index(
            index=_config.index_name_dedupe_config,
            id=urllib.parse.quote(config.name, safe=""),
            body=config.model_dump(),
            params={"timeout": _config.opensearch_api_timeout},
        )

    async def get_config(self, name: str) -> DedupeConfig:
        try:
            res = await self.opensearch_client.get_source(
                index=_config.index_name_dedupe_config,
                id=urllib.parse.quote(name, safe=""),
                params={"timeout": _config.opensearch_api_timeout},
            )
            return DedupeConfig(**res)
        except Exception:
            return None

    async def delete_config(self, name: str):
        await self.opensearch_client.delete(
            index=_config.index_name_dedupe_config,
            id=urllib.parse.quote(name, safe=""),
            params={"timeout": _config.opensearch_api_timeout},
        )
