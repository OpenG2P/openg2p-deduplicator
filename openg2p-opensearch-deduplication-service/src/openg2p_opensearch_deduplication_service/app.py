# ruff: noqa: E402

from .config import Settings

_config = Settings.get_config()

from openg2p_fastapi_common.app import Initializer as BaseInitializer

from .controllers.config_controller import DedupeConfigController
from .controllers.deduplicate_controller import DeduplicateController
from .controllers.get_duplicates_controller import GetDuplicatesController
from .controllers.health_controller import HealthController
from .services.config_service import DedupeConfigService
from .services.deduplication_service import DeduplicationService
from .services.opensearch_service import OpenSearchClientService


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().initialize(**kwargs)

        OpenSearchClientService()
        DedupeConfigService()
        DeduplicationService()
        DedupeConfigController().post_init()
        DeduplicateController().post_init()
        GetDuplicatesController().post_init()
        HealthController().post_init()

    def migrate_database(self, args):
        super().migrate_database(args)
        # Create default config set
