from datetime import datetime
from enum import Enum

from pydantic import BaseModel, field_serializer


class DeduplicationStatus(Enum):
    inprogress = "inprogress"
    completed = "completed"
    paused = "paused"
    failed = "failed"


class DeduplicateHttpRequest(BaseModel):
    doc_id: str
    dedupe_config_name: str
    wait_before_exec_secs: int = 0


class DeduplicateHttpResponse(BaseModel):
    request_id: str = ""
    status: DeduplicationStatus = None


class DedupeStatusHttpResponse(BaseModel):
    status: DeduplicationStatus = None
    status_description: str = ""
    created_at: datetime
    updated_at: datetime | None = None


class DeduplicateRequestEntry(BaseModel):
    id: str
    doc_id: str
    config_name: str
    status: DeduplicationStatus
    status_description: str
    wait_before_exec_secs: int
    created_at: datetime
    updated_at: datetime | None = None

    @field_serializer("status")
    def status_serialize(self, status: DeduplicationStatus):
        return status.value
