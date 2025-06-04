import os
from typing import Literal, TypedDict


class ConfigDict(TypedDict):
    universe_name: str
    topic_slug: str
    base_url: str
    dsn: str
    org_api: str
    stats_url: str | None
    stats_site_id: str | None
    stats_token: str | None
    api_key: str | None


ENVS_CONF: dict[Literal["prod", "demo", "local"], ConfigDict] = {
    "prod": {
        "universe_name": "univers-ecospheres",
        "topic_slug": "univers-ecospheres",
        "base_url": "https://www.data.gouv.fr",
        "dsn": os.getenv("DATABASE_URL_PROD", ""),
        "stats_url": "https://stats.data.gouv.fr/index.php",
        "stats_site_id": "299",
        "stats_token": os.getenv("STATS_TOKEN", ""),
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/organizations-prod.json",
        "api_key": os.getenv("DATAGOUV_API_KEY_PROD"),
    },
    "demo": {
        "universe_name": "ecospheres",
        "topic_slug": "univers-ecospheres",
        "base_url": "https://demo.data.gouv.fr",
        "dsn": os.getenv("DATABASE_URL", ""),
        "stats_url": None,
        "stats_site_id": None,
        "stats_token": None,
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/organizations-demo.json",
        "api_key": os.getenv("DATAGOUV_API_KEY"),
    },
    "local": {
        "universe_name": "ecospheres",
        "topic_slug": "change-title",
        "base_url": "http://dev.local:7000",
        "dsn": os.getenv("DATABASE_URL", ""),
        "stats_url": None,
        "stats_site_id": None,
        "stats_token": None,
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/organizations-demo.json",
        "api_key": os.getenv("DATAGOUV_API_KEY"),
    },
}


def get_config_value(env: str, key: str) -> str:
    if env not in ENVS_CONF:
        raise ValueError(f"Invalid environment '{env}'.")
    if key not in ENVS_CONF[env]:
        raise ValueError(f"Invalid config key '{key}' for environment '{env}'.")
    return ENVS_CONF[env][key]
