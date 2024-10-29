import os
from unittest.mock import Mock, patch

import pytest

from db import get, get_table


@patch("db.get_db", Mock(return_value=True))
def test_get_return_db_if_it_truthy():
    assert get("test") is True


@patch("dataset.connect", Mock(return_value={"database": True}))
def test_get_return_fresh_db():
    os.environ["DATABASE_URL_TEST"] = "some"
    assert get("test") == {"database": True}


def test_get_table_raise_if_received_none():
    class Mock:
        def get_table(self, table_name: str):
            return None

    with patch("db.get", return_value=Mock()):
        with pytest.raises(ValueError):
            get_table("test", "table_name")


def test_get_table_return():
    class Mock:
        def get_table(self, table_name: str):
            return {"table": True}

    with patch("db.get", return_value=Mock()):
        assert get_table("test", "table_name") == {"table": True}
