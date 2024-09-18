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


def test_base_model_get_indicators_false():
    base = BaseModel({'column_false': None})
    base.indicators = [
        {'id': 'column_false', "not": None}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': True})
    base.indicators = [
        {'id': 'column_false', "not": True}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': 'specific string'})
    base.indicators = [
        {'id': 'column_false', "not": 'specific string'}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': []})
    base.indicators = [
        {'id': 'column_false', "not": [[], None]}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': None})
    base.indicators = [
        {'id': 'column_false', "not": [[], None]}
    ]

    assert base.get_indicators() == {'has_column_false': False}


def test_base_model_get_indicators_true():
    base = BaseModel({'column_one': 'some value'})
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}

    base = BaseModel({'column_one': ''})
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}

    base = BaseModel({'column_one': 0})
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}

    base = BaseModel({'column_one': []})
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}


def test_base_model_compute_prefix_id_source_find_prefix():
    base = BaseModel({
        'harvest': {
            'remote_id': 'https://slug/final'
        }
    })

    assert base.compute_prefix_id_source() == 'https://slug/'

    base = BaseModel({
        'harvest': {
            'remote_id': 'http://slug/final'
        }
    })

    assert base.compute_prefix_id_source() == 'http://slug/'


def test_base_model_compute_prefix_id_source_bad_protocol():
    base = BaseModel({
        'harvest': {
            'remote_id': 'some string before https://slug/final'
        }
    })

    assert base.compute_prefix_id_source() == 'Préfixe manquant'

    base = BaseModel({
        'harvest': {
            'remote_id': 'ftp://slug/final'
        }
    })

    assert base.compute_prefix_id_source() == 'Préfixe manquant'


def test_base_model_compute_prefix_id_source_remote_id_missing():
    base = BaseModel({
        'harvest': {}
    })

    assert base.compute_prefix_id_source() == 'Préfixe manquant'


def test_base_model_compute_prefix_id_source_harvest_missing():
    base = BaseModel({})

    assert base.compute_prefix_id_source() == 'Préfixe manquant'


def test_base_model_compute_prefix_id_source_suffix_missing():
    base = BaseModel({
        'harvest': {
            'remote_id': 'http://slug/'
        }
    })

    assert base.compute_prefix_id_source() == 'Préfixe manquant'
