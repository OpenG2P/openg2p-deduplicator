from openg2p_fastapi_common.errors import BaseAppException


class OpenSearchClientException(BaseAppException):
    def __init__(self, message, code="G2P-DEDUPE-500", http_status_code=500, headers=None, **kw):
        return super().__init__(
            code=code, message=message, http_status_code=http_status_code, headers=headers, **kw
        )
