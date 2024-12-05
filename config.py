import os
from typing import Literal

ConfigKeys = Literal["universe_name", "topic_slug", "prefix", "dsn", "org_api"]


ENVS_CONF: dict[Literal["prod", "demo"], dict[ConfigKeys, str]] = {
    "prod": {
        "universe_name": "univers-ecospheres",
        "topic_slug": "univers-ecospheres",
        "prefix": "www",
        "dsn": os.getenv("DATABASE_URL_PROD", ""),
        # FIXME: move to main branch when available
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/feat/org-api/dist/organizations-prod.json",
    },
    "demo": {
        "universe_name": "ecospheres",
        "topic_slug": "univers-ecospheres",
        "prefix": "demo",
        "dsn": os.getenv("DATABASE_URL", ""),
        # FIXME: move to main branch when available
        "org_api": "https://raw.githubusercontent.com/ecolabdata/ecospheres-universe/refs/heads/feat/org-api/dist/organizations-demo.json",
    },
}


def get_config_value(env: str, key: ConfigKeys) -> str:
    if env not in ENVS_CONF:
        raise ValueError(f"Invalid environment '{env}'.")
    return ENVS_CONF[env][key]
