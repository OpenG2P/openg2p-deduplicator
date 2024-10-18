import logging
import time
from datetime import datetime, timedelta

from openg2p_fastapi_common.errors.http_exceptions import BadRequestError
from openg2p_fastapi_common.service import BaseService
from openg2p_fastapi_common.utils.ctx_thread import CTXThread

from ..config import Settings
from ..schemas.config_request import DedupeConfig, DedupeConfigFieldQueryType
from ..schemas.deduplicate_request import DeduplicateRequestEntry, DeduplicationStatus
from ..schemas.get_duplicates_response import StoredDuplicateEntry
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
            ).model_dump(mode="json"),
        )
        return status

    def get_dedupe_request(self, request_id: str) -> DeduplicateRequestEntry:
        res = self.opensearch_client.get_source(index=_config.index_name_dedupe_requests, id=request_id)
        return DeduplicateRequestEntry.model_validate(res)

    def update_dedupe_request(self, request_id: str, key_value: dict):
        return self.opensearch_client.update(
            index=_config.index_name_dedupe_requests,
            id=request_id,
            body={"doc": key_value},
            timeout=_config.opensearch_api_timeout,
        )

    def get_duplicates_by_doc_id(self, doc_id: str) -> list[StoredDuplicateEntry]:
        try:
            res = self.opensearch_client.search(
                index=_config.index_name_duplicates,
                timeout=_config.opensearch_api_timeout,
                body={"query": {"term": {"original_id": {"value": doc_id}}}},
            )
            res = res.get("hits", {}).get("hits", [])
        except Exception:
            res = []
        res = [StoredDuplicateEntry.model_validate(entry.get("_source", {})) for entry in res]
        return res

    def run_dedupe_job(self):
        time.sleep(_config.dedupe_runner_initial_delay_secs)
        while True:
            try:
                self.run_dedupe_task()
            except Exception as e:
                _logger.exception("Error running deduplication job" + repr(e))
                raise
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
                dedupe_request = None
                continue
            break
        if not dedupe_request:
            return
        try:
            # Get Dedupe config from request
            dedupe_config = self.dedupe_config_service.get_config(dedupe_request.config_name)
            if not dedupe_config:
                _logger.error(
                    f"No config found with the given name: {dedupe_request.config_name}. Request Id: {dedupe_request.id}"
                )
                self.update_dedupe_request(
                    dedupe_request.id,
                    {
                        "status": DeduplicationStatus.failed.value,
                        "status_description": "No config found with the given name",
                        "updated_at": datetime.now(),
                    },
                )
                return
            # Find nested duplicates with the give config and update their entries
            duplicates = self.find_duplicates_by_doc_id(
                dedupe_config, dedupe_request.id, dedupe_request.doc_id
            )
            # Create duplicate entries
            self.create_duplicate_entries(dedupe_request.doc_id, duplicates, dedupe_request.id)
            # Update request status
            self.update_dedupe_request(
                dedupe_request.id,
                {
                    "status": DeduplicationStatus.completed.value,
                    "status_description": f"Deduplication Complete. {len(duplicates or [])} found.",
                    "updated_at": datetime.now(),
                },
            )
        except Exception as e:
            _logger.exception(f"Error during deduplication for request_id: {dedupe_request.id}. {repr(e)}")
            self.update_dedupe_request(
                dedupe_request.id,
                {
                    "status": DeduplicationStatus.failed.value,
                    "status_description": f"Deduplication Failed. {repr(e)}",
                    "updated_at": datetime.now(),
                },
            )
            pass

    def find_duplicates_by_doc_id(self, dedupe_config: DedupeConfig, dedupe_request_id, doc_id):
        # Get Record with the given id
        query_time = datetime.now()
        try:
            record = self.opensearch_client.get_source(
                index=dedupe_config.index, id=doc_id, timeout=_config.opensearch_api_timeout
            )
        except Exception:
            _logger.error(f"Record not found with ID: {doc_id}. Request Id: {dedupe_request_id}")
            self.update_dedupe_request(
                dedupe_request_id,
                {
                    # TODO: discuss what status to update when record not found
                    # "status": DeduplicationStatus.failed.value,
                    "status_description": f"Record with the given ID ({doc_id}) not found. Retrying...",
                    "updated_at": query_time,
                },
            )
            return []
        # Construct match query with all fields. And search for other records
        query_list = []
        for field in dedupe_config.fields:
            if field.query_type == DedupeConfigFieldQueryType.match:
                match_query = {
                    "query": record.get(field.name),
                    "boost": field.boost,
                }
                if field.fuzziness:
                    match_query["fuzziness"] = field.fuzziness
                query_list.append({"match": {field.name: match_query}})
            elif field.query_type == DedupeConfigFieldQueryType.term:
                query_list.append(
                    {
                        "term": {
                            field.name: {
                                "value": record.get(field.name),
                                "boost": field.boost,
                                "case_insensitive": True,
                            }
                        }
                    }
                )
        duplicates_res = self.opensearch_client.search(
            body={"_source": False, "query": {"bool": {"must": query_list}}},
            index=dedupe_config.index,
            timeout=_config.opensearch_api_timeout,
        )
        duplicates_res = duplicates_res.get("hits", {}).get("hits", [])
        duplicates_res = [duplicate for duplicate in duplicates_res if duplicate.get("_id") != doc_id]
        # Update duplicates in the current record and all other records
        return duplicates_res

    def create_duplicate_entries(self, doc_id: str, duplicates: list, request_id: str):
        for dup in duplicates:
            # Create to and fro duplicate entries
            self.opensearch_client.index(
                index=_config.index_name_duplicates,
                id=f"{doc_id}{_config.duplicate_entry_id_joiner}{dup.get('_id')}",
                timeout=_config.opensearch_api_timeout,
                body=StoredDuplicateEntry(
                    original_id=doc_id,
                    duplicate_id=dup.get("_id"),
                    match_score=dup.get("_score"),
                    last_dedupe_request_id=request_id,
                ).model_dump(mode="json"),
            )
            self.opensearch_client.index(
                index=_config.index_name_duplicates,
                id=f"{dup.get('_id')}{_config.duplicate_entry_id_joiner}{doc_id}",
                timeout=_config.opensearch_api_timeout,
                body=StoredDuplicateEntry(
                    original_id=dup.get("_id"),
                    duplicate_id=doc_id,
                    match_score=dup.get("_score"),
                    last_dedupe_request_id=request_id,
                ).model_dump(mode="json"),
            )

    def is_runner_thread_alive(self):
        return self.dedupe_runner.is_alive()
