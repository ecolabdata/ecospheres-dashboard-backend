import pytest

from config import get_config_value


def test_wrong_env():
    with pytest.raises(ValueError):
        get_config_value("wrong_env", "universe_name")


def test_get_simple_value():
    assert get_config_value("demo", "universe_name") == "ecospheres"
    assert get_config_value("prod", "universe_name") == "univers-ecospheres"
