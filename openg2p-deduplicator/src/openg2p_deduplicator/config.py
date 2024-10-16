from openg2p_fastapi_common.config import Settings as BaseSettings
from pydantic_settings import SettingsConfigDict

from . import __version__


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="deduplicator_",
        env_file=".env",
        extra="allow",
    )

    openapi_title: str = "Deduplicator"
    openapi_description: str = """
    OpenG2P Deduplicator based on OpenSearch
    """
    openapi_version: str = __version__

    opensearch_url: str = "http://localhost:9200"
    opensearch_username: str = ""
    opensearch_password: str = ""
    opensearch_api_timeout: int = 10

    index_name_dedupe_configs: str = "g2p_dedupe_configs"
    index_name_dedupe_requests: str = "g2p_dedupe_requests"
    index_name_duplicates: str = "g2p_dedupe_duplicates"

    duplicate_entry_id_joiner: str = "<->"

    dedupe_runner_initial_delay_secs: int = 5
    dedupe_runner_interval_secs: int = 10
