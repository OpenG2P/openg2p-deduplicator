# ruff: noqa: E402

from .config import Settings

_config = Settings.get_config()

from openg2p_fastapi_common.app import Initializer as BaseInitializer

from .controllers.config_controller import DedupeConfigController
from .controllers.deduplicate_controller import DeduplicateController
from .controllers.get_duplicates_controller import GetDuplicatesController
from .controllers.health_controller import HealthController
from .services.config_service import ConfigService
from .services.deduplication_service import DeduplicationService


class Initializer(BaseInitializer):
    def initialize(self, **kwargs):
        super().initialize(**kwargs)

        ConfigService()
        DeduplicationService()
        DedupeConfigController().post_init()
        DeduplicateController().post_init()
        GetDuplicatesController().post_init()
        HealthController().post_init()

    def migrate_database(self, args):
        super().migrate_database(args)
        # Create default config set
