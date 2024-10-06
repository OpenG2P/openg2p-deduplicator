from enum import Enum

from pydantic import BaseModel


class DeduplicationStatus(Enum):
    inprogress = "inprogress"
    completed = "completed"
    failed = "failed"


class DeduplicateHttpRequest(BaseModel):
    doc_id: str
    dedupe_config_name: str


class DeduplicateHttpResponse(BaseModel):
    request_id: str = ""
    status: DeduplicationStatus = None


class DedupeStatusHttpResponse(BaseModel):
    status: DeduplicationStatus = None
    status_description: str = ""
