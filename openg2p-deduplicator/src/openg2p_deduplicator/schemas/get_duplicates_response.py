from pydantic import BaseModel


class StoredDuplicateEntry(BaseModel):
    original_id: str
    duplicate_id: str
    match_score: float
    last_dedupe_request_id: str


class HttpDuplicateEntry(BaseModel):
    id: str
    match_score: float
    last_dedupe_request_id: str


class GetDuplicatesHttpResponse(BaseModel):
    duplicates: list[HttpDuplicateEntry]
