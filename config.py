import os
from typing import Literal, TypedDict

import requests
from yaml import safe_load


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
    metrics_api_url: str | None
    front_config_file: str | None


ENVS_CONF: dict[Literal["prod", "demo", "local"], ConfigDict] = {
    "prod": {
        "universe_name": "univers-ecospheres",
        "topic_slug": "univers-ecospheres",
        "base_url": "https://www.data.gouv.fr",
        "dsn": os.getenv("DATABASE_URL_PROD", ""),
        "stats_url": "https://stats.data.gouv.fr/index.php",
        "stats_site_id": "299",
        "stats_token": os.getenv("STATS_TOKEN", ""),
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/ecospheres-prod/organizations-datasets.json",
        "api_key": os.getenv("DATAGOUV_API_KEY_PROD"),
        "metrics_api_url": "https://metric-api.data.gouv.fr/api",
        "front_config_file": "https://raw.githubusercontent.com/opendatateam/udata-front-kit/refs/heads/ecospheres-prod/configs/ecospheres/config.yaml",
    },
    "demo": {
        "universe_name": "ecospheres",
        "topic_slug": "univers-ecospheres",
        "base_url": "https://demo.data.gouv.fr",
        "dsn": os.getenv("DATABASE_URL", ""),
        "stats_url": None,
        "stats_site_id": None,
        "stats_token": None,
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/ecospheres-prod/organizations-datasets.json",
        "api_key": os.getenv("DATAGOUV_API_KEY"),
        "metrics_api_url": None,
        "front_config_file": "https://raw.githubusercontent.com/opendatateam/udata-front-kit/refs/heads/main/configs/ecospheres/config.yaml",
    },
    "local": {
        "universe_name": "ecospheres",
        "topic_slug": "change-title",
        "base_url": "http://dev.local:7000",
        "dsn": os.getenv("DATABASE_URL", ""),
        "stats_url": None,
        "stats_site_id": None,
        "stats_token": None,
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/main/dist/ecospheres-prod/organizations-datasets.json",
        "api_key": os.getenv("DATAGOUV_API_KEY"),
        "metrics_api_url": None,
        "front_config_file": "https://raw.githubusercontent.com/opendatateam/udata-front-kit/refs/heads/main/configs/ecospheres/config.yaml",
    },
}


def get_config_value(env: str, key: str) -> str:
    if env not in ENVS_CONF:
        raise ValueError(f"Invalid environment '{env}'.")
    if key not in ENVS_CONF[env]:
        raise ValueError(f"Invalid config key '{key}' for environment '{env}'.")
    return ENVS_CONF[env][key]


def get_front_config(env: str) -> dict:
    config_file = get_config_value(env, "front_config_file")
    r = requests.get(config_file)
    r.raise_for_status()
    return safe_load(r.content)
