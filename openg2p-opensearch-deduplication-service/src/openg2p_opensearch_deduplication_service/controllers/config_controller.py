from openg2p_fastapi_common.controller import BaseController

from ..schemas.config_request import (
    DedupeConfigDeleteHttpResponse,
    DedupeConfigGetHttpResponse,
    DedupeConfigHttpRequest,
    DedupeConfigPutHttpResponse,
)


class DedupeConfigController(BaseController):
    def __init__(self, **kw):
        super().__init__(**kw)

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

    async def put_dedupe_config(
        self, name: str, deduplicate_request: DedupeConfigHttpRequest
    ):
        pass

    async def get_dedupe_config(self, name: str):
        pass

    async def delete_dedupe_config(self, name: str):
        pass
