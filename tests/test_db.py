import os

import pytest
from unittest.mock import patch, Mock
from db import get


def test_get_raise_without_DATABASE_URL():
    with pytest.raises(ValueError):
        get()


@patch('db.get_db', Mock(return_value=True))
def test_get_return_db_if_it_truthy():
    assert get() is True


@patch('dataset.connect', Mock(return_value={'database': True}))
def test_get_return_fresh_db():
    os.environ['DATABASE_URL'] = 'some'
    assert get() == {'database': True}
