from openg2p_fastapi_common.service import BaseService
from opensearchpy import AsyncOpenSearch

from ..config import Settings
from ..exceptions.opensearch_exception import OpenSearchClientException

_config: Settings = Settings.get_config()


class OpenSearchClientService(BaseService):
    def __init__(self, **kw):
        super().__init__(**kw)

        self.client = AsyncOpenSearch(
            hosts=[_config.opensearch_url],
            basic_auth=(_config.opensearch_username, _config.opensearch_password)
            if _config.opensearch_username
            else None,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )

    async def get_source(self, index, id, params=None, headers=None, **kw):
        try:
            return await self.client.get_source(index=index, id=id, params=params, headers=headers, **kw)
        except Exception as e:
            raise OpenSearchClientException("Error while OS query. " + repr(e)) from e

    async def get(self, index, id, params=None, headers=None, **kw):
        try:
            return await self.client.get(index=index, id=id, params=params, headers=headers, **kw)
        except Exception as e:
            raise OpenSearchClientException("Error while OS query. " + repr(e)) from e

    async def search(self, body=None, index=None, params=None, headers=None, **kw):
        try:
            return await self.client.search(index=index, body=body, params=params, headers=headers, **kw)
        except Exception as e:
            raise OpenSearchClientException("Error while OS query. " + repr(e)) from e

    async def index(self, index, body, id=None, params=None, headers=None, **kw):
        try:
            return await self.client.index(
                index=index, body=body, id=id, params=params, headers=headers, **kw
            )
        except Exception as e:
            raise OpenSearchClientException("Error while OS query. " + repr(e)) from e

    async def delete(self, index, id, params=None, headers=None, **kw):
        try:
            return await self.client.delete(index=index, id=id, params=params, headers=headers, **kw)
        except Exception as e:
            raise OpenSearchClientException("Error while OS query. " + repr(e)) from e
