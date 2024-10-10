from pydantic import BaseModel


class DuplicateEntry(BaseModel):
    id: str
    match_score: float
    last_dedupe_request_id: str


class StoredDuplicates(BaseModel):
    duplicates: list[DuplicateEntry]


class GetDuplicatesHttpResponse(StoredDuplicates):
    pass
