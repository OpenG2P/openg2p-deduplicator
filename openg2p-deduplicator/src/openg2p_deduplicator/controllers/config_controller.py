from datetime import datetime

from openg2p_fastapi_common.controller import BaseController
from openg2p_fastapi_common.errors.http_exceptions import BadRequestError

from ..schemas.config_request import (
    DedupeConfig,
    DedupeConfigDeleteHttpResponse,
    DedupeConfigGetHttpResponse,
    DedupeConfigHttpRequest,
    DedupeConfigPutHttpResponse,
)
from ..services.config_service import DedupeConfigService


class DedupeConfigController(BaseController):
    def __init__(self, **kw):
        super().__init__(**kw)

        self._config_service: DedupeConfigService = None

        self.router.tags += ["config"]

        self.router.add_api_route(
            "/config/{name}",
            self.put_dedupe_config,
            responses={200: {"model": DedupeConfigPutHttpResponse}},
            methods=["PUT"],
        )

        self.router.add_api_route(
            "/config/{name}",
            self.get_dedupe_config,
            responses={200: {"model": DedupeConfigGetHttpResponse}},
            methods=["GET"],
        )

        self.router.add_api_route(
            "/config/{name}",
            self.delete_dedupe_config,
            responses={200: {"model": DedupeConfigDeleteHttpResponse}},
            methods=["DELETE"],
        )

    @property
    def config_service(self):
        if not self._config_service:
            self._config_service = DedupeConfigService.get_component()
        return self._config_service

    def put_dedupe_config(self, name: str, config_request: DedupeConfigHttpRequest):
        self.config_service.add_or_update_config(
            DedupeConfig(name=name, created_at=datetime.now(), **config_request.model_dump())
        )
        return DedupeConfigPutHttpResponse(message="Config updated.")

    def get_dedupe_config(self, name: str):
        config = self.config_service.get_config(name)
        if not config:
            raise BadRequestError(code="G2P-DEDUPE-600", message="No config found with the given name")
        return DedupeConfigGetHttpResponse(config=config)

    def delete_dedupe_config(self, name: str):
        config = self.config_service.get_config(name)
        if not config:
            raise BadRequestError(code="G2P-DEDUPE-600", message="No config found with the given name")
        self.config_service.delete_config(name)
        return DedupeConfigPutHttpResponse(message="Config deleted.")
