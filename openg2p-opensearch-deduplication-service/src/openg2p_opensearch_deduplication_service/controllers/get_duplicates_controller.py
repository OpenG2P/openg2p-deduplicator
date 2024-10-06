from openg2p_fastapi_common.controller import BaseController

from ..schemas.get_duplicates_response import GetDuplicatesHttpResponse


class GetDuplicatesController(BaseController):
    def __init__(self, **kw):
        super().__init__(**kw)

        self.router.tags += ["getDuplicates"]

        self.router.add_api_route(
            "/getDuplicates/{doc_id}",
            self.get_duplicates_by_id,
            responses={200: {"model": GetDuplicatesHttpResponse}},
            methods=["GET"],
        )

    async def get_duplicates_by_id(self, doc_id: str):
        pass
