from openg2p_fastapi_common.errors.http_exceptions import BadRequestError
from openg2p_fastapi_common.service import BaseService

from ..config import Settings
from ..schemas.deduplicate_request import DeduplicateRequestEntry, DeduplicationStatus
from ..services.config_service import DedupeConfigService
from ..services.opensearch_service import OpenSearchClientService

_config: Settings = Settings.get_config()


class DeduplicationService(BaseService):
    def __init__(self, **kw):
        super().__init__(**kw)

        self._dedupe_config_service: DedupeConfigService = None
        self._opensearch_client: OpenSearchClientService = None

    @property
    def opensearch_client(self):
        if not self._opensearch_client:
            self._opensearch_client = OpenSearchClientService.get_component()
        return self._opensearch_client

    @property
    def dedupe_config_service(self):
        if not self._dedupe_config_service:
            self._dedupe_config_service = DedupeConfigService.get_component()
        return self._dedupe_config_service

    async def create_dedupe_request(
        self, request_id: str, doc_id: str, config_name: str
    ) -> DeduplicationStatus:
        if not await self.dedupe_config_service.get_config(config_name):
            raise BadRequestError(code="G2P-DEDUPE-600", message="No config found with the given name")
        status = DeduplicationStatus.inprogress
        await self.opensearch_client.index(
            index=_config.index_name_dedupe_requests,
            id=request_id,
            body=DeduplicateRequestEntry(
                doc_id=doc_id,
                config_name=config_name,
                status=status,
                status_description="Deduplication in progress",
            ).model_dump(mode="json"),
        )
        return status

    async def get_dedupe_request_status(self, request_id: str) -> tuple[DeduplicationStatus, str]:
        res = await self.opensearch_client.get_source(index=_config.index_name_dedupe_requests, id=request_id)
        return DeduplicationStatus[res.get("status", None)], res.get("status_description", None)

    def is_runner_thread_alive(self):
        return True
