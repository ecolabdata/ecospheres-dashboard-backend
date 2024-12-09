import os
from typing import Literal

ConfigKeys = Literal[
    "universe_name", "topic_slug", "prefix", "dsn", "stats_url", "stats_site_id", "stats_token"
]


ENVS_CONF: dict[Literal["prod", "demo"], dict[ConfigKeys, str]] = {
    "prod": {
        "universe_name": "univers-ecospheres",
        "topic_slug": "univers-ecospheres",
        "prefix": "www",
        "dsn": os.getenv("DATABASE_URL_PROD", ""),
        "stats_url": "https://stats.data.gouv.fr/index.php",
        "stats_site_id": "299",
        "stats_token": os.getenv("STATS_TOKEN", ""),
    },
    "demo": {
        "universe_name": "ecospheres",
        "topic_slug": "univers-ecospheres",
        "prefix": "demo",
        "dsn": os.getenv("DATABASE_URL", ""),
        "stats_url": "",
        "stats_site_id": "",
        "stats_token": "",
    },
}


def get_config_value(env: str, key: ConfigKeys) -> str:
    if env not in ENVS_CONF:
        raise ValueError(f"Invalid environment '{env}'.")
    return ENVS_CONF[env][key]
