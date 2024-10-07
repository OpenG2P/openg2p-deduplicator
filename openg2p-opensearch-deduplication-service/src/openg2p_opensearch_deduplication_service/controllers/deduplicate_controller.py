from uuid import uuid4

from fastapi import Request
from openg2p_fastapi_common.controller import BaseController

from ..schemas.deduplicate_request import (
    DedupeStatusHttpResponse,
    DeduplicateHttpRequest,
    DeduplicateHttpResponse,
)
from ..services.deduplication_service import DeduplicationService


class DeduplicateController(BaseController):
    def __init__(self, **kw):
        super().__init__(**kw)

        self._deduplication_service: DeduplicationService = None

        self.router.tags += ["deduplicate"]

        self.router.add_api_route(
            "/deduplicate",
            self.post_deduplicate_with_id,
            responses={200: {"model": DeduplicateHttpResponse}},
            methods=["POST"],
        )

        self.router.add_api_route(
            "/deduplicate/status/{request_id}",
            self.get_deduplicate_request_status,
            responses={200: {"model": DedupeStatusHttpResponse}},
            methods=["GET"],
        )

    @property
    def deduplication_service(self):
        if not self._deduplication_service:
            self._deduplication_service = DeduplicationService.get_component()
        return self._deduplication_service

    async def post_deduplicate_with_id(self, deduplicate_request: DeduplicateHttpRequest, request: Request):
        request_id = request.cookies.get("request_id", None) or str(uuid4())
        status = await self.deduplication_service.create_dedupe_request(
            request_id, deduplicate_request.doc_id, deduplicate_request.dedupe_config_name
        )
        return DeduplicateHttpResponse(request_id=request_id, status=status)

    async def get_deduplicate_request_status(self, request_id: str):
        status, status_description = await self.deduplication_service.get_dedupe_request_status(request_id)
        return DedupeStatusHttpResponse(status=status, status_description=status_description)
