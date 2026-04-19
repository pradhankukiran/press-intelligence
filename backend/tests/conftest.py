import os

from press_intelligence.core.config import get_settings


os.environ["DATA_MODE"] = "mock"
get_settings.cache_clear()
