from openg2p_fastapi_common.controller import BaseController
from openg2p_fastapi_common.errors.http_exceptions import InternalServerError

from ..schemas.health_response import HealthResponse
from ..services.deduplication_service import DeduplicationService


class HealthController(BaseController):
    def __init__(self, **kw):
        super().__init__(**kw)

        self.deduplication_service: DeduplicationService = DeduplicationService.get_component()

        self.router.tags += ["health"]

        self.router.add_api_route(
            "/health",
            self.get_health,
            responses={200: {"model": HealthResponse}},
            methods=["GET"],
        )

    def get_health(self):
        if self.deduplication_service.is_runner_thread_alive():
            return HealthResponse(status="healthy")
        raise InternalServerError(
            code="G2P-DEDUP-500",
            message="Deduplication Service Job is inactive.",
        )
