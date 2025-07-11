import json

import pytest

from models import Bouquet, Dataset, DatasetComputedColumns, ResourceComputedColumns


@pytest.fixture
def fixture_payload(request):
    with open(f"tests/fixtures/{request.param}", "r") as file:
        data = json.load(file)
    return data


def test_computed_get_attr_by_path_return_none_on_keyerror():
    base = DatasetComputedColumns({"some": {"another_path": 2}}, prefix="test")

    assert base.get_attr_by_path("some__path") is None


def test_computed_get_attr_by_path_ignore_none_value():
    base = DatasetComputedColumns({"some": {"path": None}}, prefix="test")

    assert base.get_attr_by_path("some__path") is None


def test_computed_get_attr_by_path_find_sub_property():
    base = DatasetComputedColumns({"foo": {"bar": 1}}, prefix="test")

    assert base.get_attr_by_path("foo__bar") == 1


def test_computed_get_indicators_false():
    base = DatasetComputedColumns({"column_false": None}, prefix="test")
    base.indicators = [{"field": "column_false", "exclude": (None,)}]

    assert base.get_indicators() == {"has_column_false": False}

    base = DatasetComputedColumns({"column_false": True}, prefix="test")
    base.indicators = [{"field": "column_false", "exclude": (True,)}]

    assert base.get_indicators() == {"has_column_false": False}

    base = DatasetComputedColumns({"column_false": "specific string"}, prefix="test")
    base.indicators = [{"field": "column_false", "exclude": ("specific string",)}]

    assert base.get_indicators() == {"has_column_false": False}

    base = DatasetComputedColumns({"column_false": []}, prefix="test")
    base.indicators = [{"field": "column_false", "exclude": ([], None)}]

    assert base.get_indicators() == {"has_column_false": False}

    base = DatasetComputedColumns({"column_false": None}, prefix="test")
    base.indicators = [{"field": "column_false", "exclude": ([], None)}]

    assert base.get_indicators() == {"has_column_false": False}


def test_computed_get_indicators_true():
    base = DatasetComputedColumns({"column_one": "some value"}, prefix="test")
    base.indicators = [{"field": "column_one", "exclude": (None,)}]

    assert base.get_indicators() == {"has_column_one": True}

    base = DatasetComputedColumns({"column_one": ""}, prefix="test")
    base.indicators = [{"field": "column_one", "exclude": (None,)}]

    assert base.get_indicators() == {"has_column_one": True}

    base = DatasetComputedColumns({"column_one": 0}, prefix="test")
    base.indicators = [{"field": "column_one", "exclude": (None,)}]

    assert base.get_indicators() == {"has_column_one": True}

    base = DatasetComputedColumns({"column_one": []}, prefix="test")
    base.indicators = [{"field": "column_one", "exclude": (None,)}]

    assert base.get_indicators() == {"has_column_one": True}


def test_computed_get_prefix_or_fallback_from_find_prefix():
    base = DatasetComputedColumns({"harvest": {"remote_id": "https://slug/final"}}, prefix="test")

    assert base.get_prefix_or_fallback_from("remote_id") == "https://slug/"

    base = DatasetComputedColumns({"harvest": {"remote_id": "http://slug/final"}}, prefix="test")

    assert base.get_prefix_or_fallback_from("remote_id") == "http://slug/"


def test_computed_get_prefix_or_fallback_from_string_ending_with_slash():
    base = DatasetComputedColumns(
        {"harvest": {"remote_id": "bépobépobépobépo/final"}}, prefix="test"
    )

    assert base.get_prefix_or_fallback_from("remote_id") == "bépobépobépobépo/"


def test_computed_get_prefix_or_fallback_from_with_none_harvest():
    base = DatasetComputedColumns({"harvest": None}, prefix="test")

    assert (
        base.get_prefix_or_fallback_from("remote_id")
        == DatasetComputedColumns.MISSING_PREFIX_MESSAGE
    )


def test_computed_get_prefix_or_fallback_from_remote_id_missing():
    base = DatasetComputedColumns({"harvest": {}}, prefix="test")

    assert (
        base.get_prefix_or_fallback_from("remote_id")
        == DatasetComputedColumns.MISSING_PREFIX_MESSAGE
    )


def test_computed_get_prefix_or_fallback_from_harvest_missing():
    base = DatasetComputedColumns({}, prefix="test")

    assert (
        base.get_prefix_or_fallback_from("remote_id")
        == DatasetComputedColumns.MISSING_PREFIX_MESSAGE
    )


def test_computed_get_prefix_or_fallback_from_suffix_missing():
    base = DatasetComputedColumns({"harvest": {"remote_id": "http://slug/"}}, prefix="test")

    assert (
        base.get_prefix_or_fallback_from("remote_id")
        == DatasetComputedColumns.MISSING_PREFIX_MESSAGE
    )


def test_computed_get_url_data_gouv():
    base = DatasetComputedColumns({"dataset_id": "123456"}, prefix="test")

    assert base.get_url_data_gouv() == (
        '<a href="https://test.data.gouv.fr/fr/datasets/123456"' ' target="_blank">123456</a>'
    )


def test_computed_get_consistent_dates_updated_in_the_future():
    base = DatasetComputedColumns({"created_at": "100", "last_modified": "200"}, prefix="test")

    assert base.get_consistent_dates() is True


def test_computed_get_consistent_dates_updated_in_the_past():
    base = DatasetComputedColumns({"created_at": "300", "last_modified": "100"}, prefix="test")

    assert base.get_consistent_dates() is False


def test_computed_get_consistent_dates_missing_modified():
    base = DatasetComputedColumns({"created_at": "400"}, prefix="test")

    assert base.get_consistent_dates() is True


def test_computed_get_consistent_dates_missing_created():
    base = DatasetComputedColumns({"last_modified": "400"}, prefix="test")

    assert base.get_consistent_dates() is False


def test_computed_get_consistent_dates_no_dates():
    base = DatasetComputedColumns({}, prefix="test")

    assert base.get_consistent_dates() is True


def test_computed_get_consistent_temporal_coverage_end_in_the_future():
    base = DatasetComputedColumns({"temporal_coverage": {"start": 1, "end": 2}}, prefix="test")

    assert base.get_consistent_temporal_coverage() is True


def test_computed_get_consistent_temporal_coverage_end_in_the_past():
    base = DatasetComputedColumns({"temporal_coverage": {"start": 4, "end": 3}}, prefix="test")

    assert base.get_consistent_temporal_coverage() is False


def test_computed_get_consistent_temporal_coverage_missing_end():
    base = DatasetComputedColumns({"temporal_coverage": {"start": 4}}, prefix="test")

    assert base.get_consistent_temporal_coverage() is False


def test_computed_get_consistent_temporal_coverage_missing_start():
    base = DatasetComputedColumns({"temporal_coverage": {"end": 4}}, prefix="test")

    assert base.get_consistent_temporal_coverage() is False


def test_computed_get_consistent_temporal_coverage_no_dates():
    base = DatasetComputedColumns({"temporal_coverage": {}}, prefix="test")

    assert base.get_consistent_temporal_coverage() is True


@pytest.mark.parametrize("fixture_payload", ["payload_ok.json"], indirect=["fixture_payload"])
def test_computed_harvest_spread(fixture_payload):
    base = DatasetComputedColumns(fixture_payload, prefix="test")

    actual = base.get_harvest_info(
        [k for k in Dataset.__dict__.keys() if k.startswith("harvest__")]
    )

    expected = {
        "harvest__backend": "CSW-DCAT",
        "harvest__created_at": "2013-02-16T00:00:00+00:00",
        "harvest__dct_identifier": "4b112795-181a-4af5-9f66-c0837f50cbfa",
        "harvest__domain": "catalogue.geo-ide.developpement-durable.gouv.fr",
        "harvest__last_update": "2024-05-24T02:52:01.047000+00:00",
        # no modified_at
        "harvest__remote_id": "4b112795-181a-4af5-9f66-c0837f50cbfa",
        "harvest__remote_url": "https://catalogue.geo-ide.developpement-durable.gouv.fr:8443//catalogue/resource/4b112795-181a-4af5-9f66-c0837f50cbfa",
        "harvest__source_id": "65390755494c3b6fb40892ec",
        "harvest__uri": "https://catalogue.geo-ide.developpement-durable.gouv.fr:8443//catalogue/resource/4b112795-181a-4af5-9f66-c0837f50cbfa",
    }

    assert {k: v for k, v in actual.items() if k.startswith("harvest__")} == expected


@pytest.mark.parametrize("fixture_payload", ["payload_ok.json"], indirect=["fixture_payload"])
def test_computed_harvest_spread_with_harvest_none(fixture_payload):
    try:
        fixture_payload["harvest"] = None
        payload_with_empty_harvest = fixture_payload

        base = DatasetComputedColumns(payload_with_empty_harvest, prefix="test")
        base.get_harvest_info([k for k in Dataset.__dict__.keys() if k.startswith("harvest__")])
    except Exception:
        pytest.fail()


def test_computed_get_license_title_not_found_key():
    base = DatasetComputedColumns({}, prefix="test", licenses=[{"id": "foo", "title": "bar"}])
    assert base.get_license_title() is None


def test_computed_get_license_title_found_key():
    base = DatasetComputedColumns(
        {"license": "foo"}, prefix="test", licenses=[{"id": "foo", "title": "bar"}]
    )
    assert base.get_license_title() == "bar"


@pytest.mark.parametrize(
    "fixture_payload", ["resource_payload_ok.json"], indirect=["fixture_payload"]
)
def test_resource_model_indicators(fixture_payload):
    actual = ResourceComputedColumns(fixture_payload).get_indicators()

    expected = {
        "title__exists": True,
        "description__exists": False,
        "type__exists": True,
        "format__exists": False,
    }

    assert actual | expected == actual  # type: ignore


@pytest.mark.parametrize(
    "fixture_payload", ["bouquet_payload_ok.json"], indirect=["fixture_payload"]
)
def test_bouquet_theme(fixture_payload):
    bouquet = Bouquet.from_payload(
        fixture_payload,
        themes={"ecospheres-theme-mieux-se-deplacer": "Mieux se déplacer"},
    )
    assert bouquet.theme == "Mieux se déplacer"
