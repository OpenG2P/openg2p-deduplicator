from openg2p_fastapi_common.service import BaseService


class DeduplicationService(BaseService):
    def __init__(self, **kw):
        super().__init__(**kw)

    def is_runner_thread_alive(self):
        return True
