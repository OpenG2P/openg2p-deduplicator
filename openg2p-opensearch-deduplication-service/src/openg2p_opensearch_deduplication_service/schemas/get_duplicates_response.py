from pydantic import BaseModel


class DuplicateEntry(BaseModel):
    id: str
    match_score: str


class GetDuplicatesHttpResponse(BaseModel):
    duplicates: list[DuplicateEntry]
