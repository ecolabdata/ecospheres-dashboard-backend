import os

import dataset
import dataset.table
from dataset.util import ResultIter

_db = None


def get() -> dataset.Database:
    global _db
    if _db:
        return _db
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise ValueError("Required DATABASE_URL env var missing.")
    _db = dataset.connect(dsn)
    return _db


def get_table(table_name: str) -> dataset.table.Table:
    table = get().get_table(table_name)
    if table is None:
        raise ValueError(f"Table '{table_name}' does not exist.")
    return table


def get_tables() -> list[str]:
    return get().tables


def query(q: str, *args, **kwargs) -> ResultIter:
    return get().query(q, *args, **kwargs)
