from openg2p_fastapi_common.controller import BaseController

from ..schemas.deduplicate_request import (
    DedupeStatusHttpResponse,
    DeduplicateHttpRequest,
    DeduplicateHttpResponse,
)


class DeduplicateController(BaseController):
    def __init__(self, **kw):
        super().__init__(**kw)

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

    async def post_deduplicate_with_id(
        self, deduplicate_request: DeduplicateHttpRequest
    ):
        pass

    async def get_deduplicate_request_status(self, request_id: str):
        pass
