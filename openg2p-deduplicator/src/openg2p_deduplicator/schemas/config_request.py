from datetime import datetime
from enum import Enum

from openg2p_fastapi_common.errors.http_exceptions import BadRequestError
from pydantic import BaseModel, field_validator, model_validator


class DedupeConfigFieldQueryType(Enum):
    term = "term"
    match = "match"


class DedupeConfigField(BaseModel):
    name: str
    query_type: DedupeConfigFieldQueryType = DedupeConfigFieldQueryType.match
    fuzziness: str | None = None
    boost: float = 1

    @model_validator(mode="after")
    def check_fuzziness_with_query_type(self):
        if self.query_type == DedupeConfigFieldQueryType.term and self.fuzziness:
            raise BadRequestError(
                code="G2P-DEDUP-400", message="Fuzziness cannot be assigned for term queries."
            )
        return self


class DedupeConfig(BaseModel):
    name: str
    active: bool
    index: str
    fields: list[DedupeConfigField]
    score_threshold: float | None = None
    created_at: datetime


class DedupeConfigHttpRequest(BaseModel):
    index: str
    fields: list[DedupeConfigField]
    score_threshold: float | None = None
    active: bool = True

    @field_validator("fields")
    @classmethod
    def fields_validator(cls, fields):
        if not fields:
            raise BadRequestError(code="G2P-DEDUP-400", message="fields list cannot be empty or null.")
        return fields


class DedupeConfigGetHttpResponse(BaseModel):
    config: DedupeConfig


class DedupeConfigPutHttpResponse(BaseModel):
    message: str


class DedupeConfigDeleteHttpResponse(BaseModel):
    message: str
