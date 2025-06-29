import os
from typing import Literal, TypedDict


class ConfigDict(TypedDict):
    universe_name: str
    topic_slug: str
    prefix: str
    dsn: str
    org_api: str
    stats_url: str | None
    stats_site_id: str | None
    stats_token: str | None
    metrics_api_url: str | None


ENVS_CONF: dict[Literal["prod", "demo"], ConfigDict] = {
    "prod": {
        "universe_name": "univers-ecospheres",
        "topic_slug": "univers-ecospheres",
        "prefix": "www",
        "dsn": os.getenv("DATABASE_URL_PROD", ""),
        "stats_url": "https://stats.data.gouv.fr/index.php",
        "stats_site_id": "299",
        "stats_token": os.getenv("STATS_TOKEN", ""),
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/organizations-prod.json",
        "metrics_api_url": "https://metric-api.data.gouv.fr/api",
    },
    "demo": {
        "universe_name": "ecospheres",
        "topic_slug": "univers-ecospheres",
        "prefix": "demo",
        "dsn": os.getenv("DATABASE_URL", ""),
        "stats_url": None,
        "stats_site_id": None,
        "stats_token": None,
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/organizations-demo.json",
        "metrics_api_url": None,
    },
}


def get_config_value(env: str, key: str) -> str:
    if env not in ENVS_CONF:
        raise ValueError(f"Invalid environment '{env}'.")
    if key not in ENVS_CONF[env]:
        raise ValueError(f"Invalid config key '{key}' for environment '{env}'.")
    return ENVS_CONF[env][key]
