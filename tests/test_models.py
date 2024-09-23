from models import BaseModel


def test_base_model_get_attr_by_path_return_none_on_keyerror():
    base = BaseModel({'some': {'another_path': 2}}, prefix='test')

    assert base.get_attr_by_path('some__path') is None


def test_base_model_get_attr_by_path_ignore_none_value():
    base = BaseModel({'some': {'path': None}}, prefix='test')

    assert base.get_attr_by_path('some__path') is None


def test_base_model_get_attr_by_path_find_sub_property():
    base = BaseModel({'foo': {'bar': 1}}, prefix='test')

    assert base.get_attr_by_path('foo__bar') == 1


def test_base_model_get_indicators_false():
    base = BaseModel({'column_false': None}, prefix='test')
    base.indicators = [
        {'id': 'column_false', "not": None}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': True}, prefix='test')
    base.indicators = [
        {'id': 'column_false', "not": True}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': 'specific string'}, prefix='test')
    base.indicators = [
        {'id': 'column_false', "not": 'specific string'}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': []}, prefix='test')
    base.indicators = [
        {'id': 'column_false', "not": [[], None]}
    ]

    assert base.get_indicators() == {'has_column_false': False}

    base = BaseModel({'column_false': None}, prefix='test')
    base.indicators = [
        {'id': 'column_false', "not": [[], None]}
    ]

    assert base.get_indicators() == {'has_column_false': False}


def test_base_model_get_indicators_true():
    base = BaseModel({'column_one': 'some value'}, prefix='test')
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}

    base = BaseModel({'column_one': ''}, prefix='test')
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}

    base = BaseModel({'column_one': 0}, prefix='test')
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}

    base = BaseModel({'column_one': []}, prefix='test')
    base.indicators = [
        {'id': 'column_one', "not": None}
    ]

    assert base.get_indicators() == {'has_column_one': True}


def test_base_model_compute_prefix_harvest_remote_id_find_prefix():
    base = BaseModel({
        'harvest': {
            'remote_id': 'https://slug/final'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_id() == 'https://slug/'

    base = BaseModel({
        'harvest': {
            'remote_id': 'http://slug/final'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_id() == 'http://slug/'


def test_base_model_compute_prefix_harvest_remote_id_string_ending_with_slash():
    base = BaseModel({
        'harvest': {
            'remote_id': 'bépobépobépobépo/final'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_id() == 'bépobépobépobépo/'


def test_base_model_compute_prefix_harvest_remote_id_remote_id_missing():
    base = BaseModel({
        'harvest': {}
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_id() == 'Préfixe manquant'


def test_base_model_compute_prefix_harvest_remote_id_harvest_missing():
    base = BaseModel({}, prefix='test')

    assert base.compute_prefix_harvest_remote_id() == 'Préfixe manquant'


def test_base_model_compute_prefix_harvest_remote_id_suffix_missing():
    base = BaseModel({
        'harvest': {
            'remote_id': 'http://slug/'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_id() == 'Préfixe manquant'


def test_base_model_compute_harvest_prefix_url_find_prefix():
    base = BaseModel({
        'harvest': {
            'remote_url': 'https://slug/final'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_url() == 'https://slug/'

    base = BaseModel({
        'harvest': {
            'remote_url': 'http://slug/final'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_url() == 'http://slug/'


def test_base_model_compute_harvest_prefix_url_weild_prefix():
    base = BaseModel({
        'harvest': {
            'remote_url': 'some string before https://slug/final'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_url() == 'some string before https://slug/'

    base = BaseModel({
        'harvest': {
            'remote_url': 'ftp://slug/final'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_url() == 'ftp://slug/'


def test_base_model_compute_harvest_prefix_url_remote_id_missing():
    base = BaseModel({
        'harvest': {}
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_url() == 'Préfixe manquant'


def test_base_model_compute_harvest_prefix_url_harvest_missing():
    base = BaseModel({}, prefix='test')

    assert base.compute_prefix_harvest_remote_url() == 'Préfixe manquant'


def test_base_model_compute_harvest_prefix_url_suffix_missing():
    base = BaseModel({
        'harvest': {
            'remote_url': 'http://slug/'
        }
    }, prefix='test')

    assert base.compute_prefix_harvest_remote_url() == 'Préfixe manquant'


def test_base_model_get_url_data_gouv():
    base = BaseModel({'id': '123456'}, prefix='test')

    assert base.get_url_data_gouv() == (
        '<a href="https://test.data.gouv.fr/fr/datasets/123456"'
        ' target="_blank">123456</a>'
    )


def test_base_model_get_consistent_dates_updated_in_the_future():
    base = BaseModel({'created_at': '100', 'last_modified': '200'}, prefix='test')

    assert base.get_consistent_dates() is True


def test_base_model_get_consistent_dates_updated_in_the_past():
    base = BaseModel({'created_at': '300', 'last_modified': '100'}, prefix='test')

    assert base.get_consistent_dates() is False


def test_base_model_get_consistent_dates_missing_modified():
    base = BaseModel({'created_at': '400'}, prefix='test')

    assert base.get_consistent_dates() is True


def test_base_model_get_consistent_dates_missing_created():
    base = BaseModel({'last_modified': '400'}, prefix='test')

    assert base.get_consistent_dates() is False


def test_base_model_get_consistent_dates_no_dates():
    base = BaseModel({}, prefix='test')

    assert base.get_consistent_dates() is False


def test_base_model_get_consistent_temporal_coverage_end_in_the_future():
    base = BaseModel({'temporal_coverage': {'start': 1, 'end': 2}}, prefix='test')

    assert base.get_consistent_temporal_coverage() is True


def test_base_model_get_consistent_temporal_coverage_end_in_the_past():
    base = BaseModel({'temporal_coverage': {'start': 4, 'end': 3}}, prefix='test')

    assert base.get_consistent_temporal_coverage() is False


def test_base_model_get_consistent_temporal_coverage_missing_end():
    base = BaseModel({'temporal_coverage': {'start': 4}}, prefix='test')

    assert base.get_consistent_temporal_coverage() is False


def test_base_model_get_consistent_temporal_coverage_missing_start():
    base = BaseModel({'temporal_coverage': {'end': 4}}, prefix='test')

    assert base.get_consistent_temporal_coverage() is False


def test_base_model_get_consistent_temporal_coverage_no_dates():
    base = BaseModel({'temporal_coverage': {}}, prefix='test')

    assert base.get_consistent_temporal_coverage() is False
