from openg2p_fastapi_common.config import Settings as BaseSettings
from pydantic_settings import SettingsConfigDict

from . import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="os_dedupe_",
        env_file=".env",
        extra="allow",
    )

    openapi_title: str = "Deduplication Service"
    openapi_description: str = """
    OpenG2P Registry deduplication based on OpenSearch
    """
    openapi_version: str = __version__
