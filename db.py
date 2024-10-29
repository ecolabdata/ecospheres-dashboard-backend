import os

import dataset
import dataset.table
from dataset.util import ResultIter

_dbs = {}


def get(env: str) -> dataset.Database:
    if env == "demo":
        dsn = os.getenv("DATABASE_URL")
    elif env == "prod":
        dsn = os.getenv("DATABASE_URL_PROD")
    elif env == "test":
        dsn = os.getenv("DATABASE_URL_TEST")
    else:
        raise ValueError(f"Invalid environment '{env}'.")

    db = get_db(env)
    if db:
        return db

    if not dsn:
        raise ValueError(f"Required database dsn env var missing for environment '{env}'.")

    return dataset.connect(dsn)


def get_db(env: str) -> dataset.Database | None:
    return _dbs.get(env)


def get_table(env: str, table_name: str) -> dataset.table.Table:
    table = get(env).get_table(table_name)

    if table is None:
        raise ValueError(f"Table '{table_name}' does not exist.")

    return table


def get_tables(env: str) -> list[str]:
    return get(env).tables


def query(env: str, q: str, *args, **kwargs) -> ResultIter:
    return get(env).query(q, *args, **kwargs)
