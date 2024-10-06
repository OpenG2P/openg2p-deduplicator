from openg2p_fastapi_common.service import BaseService
from opensearchpy import AsyncOpenSearch

from ..config import Settings

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
