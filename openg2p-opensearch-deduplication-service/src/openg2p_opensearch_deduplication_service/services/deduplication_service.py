import logging
import time
from datetime import datetime, timedelta

from openg2p_fastapi_common.errors.http_exceptions import BadRequestError
from openg2p_fastapi_common.service import BaseService
from openg2p_fastapi_common.utils.ctx_thread import CTXThread

from ..config import Settings
from ..exceptions.dedupe_runner_exception import DedupeRunnerException
from ..schemas.config_request import DedupeConfig
from ..schemas.deduplicate_request import DeduplicateRequestEntry, DeduplicationStatus
from ..schemas.get_duplicates_response import DuplicateEntry, StoredDuplicates
from ..services.config_service import DedupeConfigService
from ..services.opensearch_service import OpenSearchClientService

_config: Settings = Settings.get_config()
_logger = logging.getLogger(_config.logging_default_logger_name)


class DeduplicationService(BaseService):
    def __init__(self, **kw):
        super().__init__(**kw)

        self._dedupe_config_service: DedupeConfigService = None
        self._opensearch_client: OpenSearchClientService = None

        self.dedupe_runner = CTXThread(target=self.run_dedupe_job, daemon=True)
        self.dedupe_runner.start()

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

    def create_dedupe_request(
        self,
        request_id: str,
        doc_id: str,
        config_name: str,
        wait_before_exec_secs: int = 0,
    ) -> DeduplicationStatus:
        if not self.dedupe_config_service.get_config(config_name):
            raise BadRequestError(code="G2P-DEDUPE-600", message="No config found with the given name")
        status = DeduplicationStatus.inprogress
        self.opensearch_client.index(
            index=_config.index_name_dedupe_requests,
            id=request_id,
            body=DeduplicateRequestEntry(
                id=request_id,
                doc_id=doc_id,
                config_name=config_name,
                status=status,
                status_description="Deduplication in progress",
                wait_before_exec_secs=wait_before_exec_secs,
                created_at=datetime.now(),
            ).model_dump(),
        )
        return status

    def get_dedupe_request_status(self, request_id: str) -> tuple[DeduplicationStatus, str]:
        res = self.opensearch_client.get_source(index=_config.index_name_dedupe_requests, id=request_id)
        return DeduplicationStatus[res.get("status", None)], res.get("status_description", None)

    def get_duplicates_by_doc_id(self, doc_id: str) -> StoredDuplicates:
        try:
            res = self.opensearch_client.get_source(
                index=_config.index_name_duplicates,
                id=doc_id,
                timeout=_config.opensearch_api_timeout,
            )
        except Exception:
            res = {"duplicates": []}
        res = StoredDuplicates.model_validate(res)
        return res

    def run_dedupe_job(self):
        time.sleep(_config.dedupe_runner_initial_delay_secs)
        while True:
            try:
                self.run_dedupe_task()
            except Exception as e:
                _logger.exception("Error running deduplication job" + repr(e))
            time.sleep(_config.dedupe_runner_interval_secs)

    def run_dedupe_task(self):
        # Get Dedupe Requests
        try:
            res = self.opensearch_client.search(
                body={
                    "query": {"term": {"status": DeduplicationStatus.inprogress.value}},
                    "sort": [{"created_at": {"order": "asc"}}],
                },
                index=_config.index_name_dedupe_requests,
                timeout=_config.opensearch_api_timeout,
            )
        except Exception:
            res = {}
        res = res.get("hits", {}).get("hits", [])
        query_time = datetime.now()
        dedupe_request: DeduplicateRequestEntry = None
        for request in res:
            dedupe_request = DeduplicateRequestEntry.model_validate(request.get("_source"))
            if (
                dedupe_request.created_at + timedelta(seconds=dedupe_request.wait_before_exec_secs)
                > query_time
            ):
                continue
            break
        if not dedupe_request:
            return
        # Get Dedupe config from request
        dedupe_config = self.dedupe_config_service.get_config(dedupe_request.config_name)
        if not dedupe_config:
            _logger.error(
                f"No config found with the given name: {dedupe_request.config_name}. Request Id: {dedupe_request.id}"
            )
            self.update_dedupe_request(dedupe_request.id, {"status": DeduplicationStatus.failed.value})
            return
        # Find nested duplicates with the give config and update their entries
        self.find_and_update_duplicates_by_doc_id(dedupe_config, dedupe_request.id, dedupe_request.doc_id)
        # Update request status
        self.update_dedupe_request(dedupe_request.id, {"status": DeduplicationStatus.completed.value})

    def find_and_update_duplicates_by_doc_id(
        self, dedupe_config: DedupeConfig, dedupe_request_id, doc_id, already_updated_docs: list = None
    ):
        # Get Record with the given id
        query_time = datetime.now()
        try:
            record = self.opensearch_client.get_source(
                index=dedupe_config.index, id=doc_id, timeout=_config.opensearch_api_timeout
            )
        except Exception:
            _logger.error(f"Record not found with ID: {doc_id}. Request Id: {dedupe_request_id}")
            # TODO: discuss what status to update when record not found
            # self.update_dedupe_request(dedupe_request.id, {"status": DeduplicationStatus.failed.value})
            self.update_dedupe_request(dedupe_request_id, {"created_at": query_time})
            return
        # Construct match query with all fields. And search for other records
        match_query = []
        for field in dedupe_config.fields:
            query = {
                "query": record.get(field.name),
                "boost": field.boost,
            }
            if field.fuzziness:
                query["fuzziness"] = field.fuzziness
            match_query.append(
                {
                    "match": {
                        field.name: query
                    }
                }
            )
        duplicates_res = self.opensearch_client.search(
            body={"_source": False, "query": {"bool": {"must": match_query}}},
            index=dedupe_config.index,
            timeout=_config.opensearch_api_timeout,
        )
        duplicates_res = duplicates_res.get("hits", {}).get("hits", [])
        duplicates_res = [duplicate for duplicate in duplicates_res if duplicate.get("_id") != doc_id]
        # Update duplicates in the current record and all other records
        self.opensearch_client.index(
            index=_config.index_name_duplicates,
            id=doc_id,
            body=StoredDuplicates(
                duplicates=[
                    DuplicateEntry(
                        id=duplicate.get("_id"),
                        match_score=duplicate.get("_score"),
                        last_dedupe_request_id=dedupe_request_id,
                    )
                    for duplicate in duplicates_res
                ]
            ).model_dump(),
            timeout=_config.opensearch_api_timeout,
        )
        already_updated_docs = already_updated_docs or []
        already_updated_docs.append(doc_id)
        for duplicate in duplicates_res:
            duplicate_id = duplicate.get("_id")
            if duplicate_id not in already_updated_docs:
                self.find_and_update_duplicates_by_doc_id(
                    dedupe_config, dedupe_request_id, duplicate_id, already_updated_docs
                )

    def update_dedupe_request(self, request_id: str, key_value: dict):
        return self.opensearch_client.update(
            index=_config.index_name_dedupe_requests,
            id=request_id,
            body={"doc": key_value},
            timeout=_config.opensearch_api_timeout,
        )

    def is_runner_thread_alive(self):
        return self.dedupe_runner.is_alive()
