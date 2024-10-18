from openg2p_fastapi_common.controller import BaseController

from ..schemas.get_duplicates_response import GetDuplicatesHttpResponse, HttpDuplicateEntry
from ..services.deduplication_service import DeduplicationService


class GetDuplicatesController(BaseController):
    def __init__(self, **kw):
        super().__init__(**kw)

        self._deduplicate_service: DeduplicationService = None

        self.router.tags += ["getDuplicates"]

        self.router.add_api_route(
            "/getDuplicates/{doc_id}",
            self.get_duplicates_by_id,
            responses={200: {"model": GetDuplicatesHttpResponse}},
            methods=["GET"],
        )

    @property
    def deduplication_service(self):
        if not self._deduplicate_service:
            self._deduplicate_service = DeduplicationService.get_component()
        return self._deduplicate_service

    def get_duplicates_by_id(self, doc_id: str):
        res = self.deduplication_service.get_duplicates_by_doc_id(doc_id=doc_id)
        res = [
            HttpDuplicateEntry(
                id=entry.duplicate_id,
                match_score=entry.match_score,
                last_dedupe_request_id=entry.last_dedupe_request_id,
            )
            for entry in res
        ]
        return GetDuplicatesHttpResponse(duplicates=res)
