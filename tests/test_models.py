from models import BaseModel


def test_base_model_get_attr_by_path_return_none_on_keyerror():
    base = BaseModel({'some': {'another_path': 2}})

    assert base.get_attr_by_path('some__path') is None


def test_base_model_get_attr_by_path_ignore_none_value():
    base = BaseModel({'some': {'path': None}})

    assert base.get_attr_by_path('some__path') is None


def test_base_model_get_attr_by_path_find_sub_property():
    base = BaseModel({'foo': {'bar': 1}})

    assert base.get_attr_by_path('foo__bar') == 1
