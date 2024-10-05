from pydantic import BaseModel


class DeduplicateHttpRequest(BaseModel):
    pass


class DeduplicateHttpResponse(BaseModel):
    pass


class DedupeStatusHttpResponse(BaseModel):
    pass
