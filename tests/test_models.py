import json
from collections import defaultdict
from collections.abc import Callable, Iterable
from math import ulp

import pytest

from models import (
    Bouquet,
    ContactPoint,
    Dataset,
    DatasetComputedColumns,
    Resource,
    ResourceComputedColumns,
)


@pytest.fixture
def fixture_payload(request):
    with open(f"tests/fixtures/{request.param}", "r") as file:
        data = json.load(file)
    return data


def upper_bounds_generator[T: (int, float)](
    bounds: list[T], epsilon: Callable[[T], T], open: bool
) -> Iterable[tuple[T, int, str]]:
    """
    Will generate lists such as:

    # bounds = [.33, .66, 1.], epsilon = lambda v: v + ulp(v), open = False
    [(0.0, 0, "moins de 0.33"), (5e-324, 0, , "moins de 0.33"), (0.166..., 0, "moins de 0.33"), (0.329..., 0, "moins de 0.33"),
     (0.33, 1, "moins de 0.66"), ...
     (0.66, 2, "moins de 1.0"), ...
     (1.0, 3, "1.0")]

    # bounds = [10, 20, 30], epsilon = lambda v: 1, open = True
    [(0, 0, "moins de 10"), (1, 0, "moins de 10"), (5, 0, "moins de 10"), (9, 0, "moins de 10"),
     (10, 1, "moins de 20"), ...
     (20, 2, "moins de 30"), ...
     (30, 3, "au moins 30"), (31, 3, "au moins 30")]
    """
    max_value = bounds[-1]
    cast = type(max_value)
    for bin, (lower, upper) in enumerate(zip([cast(0)] + bounds, bounds)):
        label = f"moins de {upper}"
        yield (lower, bin, label)
        yield (lower + epsilon(lower), bin, label)
        yield (cast(lower + (upper - lower) / 2), bin, label)
        yield (upper - epsilon(upper), bin, label)
    max_bin = len(bounds)
    if open:
        label = f"au moins {max_value}"
        yield (max_value, max_bin, label)
        yield (max_value + epsilon(max_value), max_bin, label)
    else:
        yield (max_value, max_bin, f"{max_value}")


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


def test_computed_get_year_from_harvest_date():
    base = DatasetComputedColumns(
        {
            "harvest": {
                "created_at": "2024-07-03T22:30:34.295000+00:00",
                "modified_at": "2025-07-09T04:48:40.999000+00:00",
            }
        },
        prefix="test",
    )

    assert base.get_year_from_harvest_date("created_at") == 2024
    assert base.get_year_from_harvest_date("modified_at") == 2025


def test_computed_get_year_from_harvest_missing():
    base = DatasetComputedColumns({"harvest": {}}, prefix="test")

    assert base.get_year_from_harvest_date("created_at") is None
    assert base.get_year_from_harvest_date("modified_at") is None


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


def test_computed_get_temporal_coverage_range():
    base = DatasetComputedColumns(
        {"temporal_coverage": {"start": "2024-07-03", "end": "2025-07-09"}},
        prefix="test",
    )

    assert base.get_temporal_coverage_range() == "2024-07-03 - 2025-07-09"


def test_computed_get_temporal_coverage_range_start_only():
    base = DatasetComputedColumns(
        {"temporal_coverage": {"start": "2024-07-03"}},
        prefix="test",
    )

    assert base.get_temporal_coverage_range() == "2024-07-03 - ?"


def test_computed_get_temporal_coverage_range_end_only():
    base = DatasetComputedColumns(
        {"temporal_coverage": {"end": "2025-07-09"}},
        prefix="test",
    )

    assert base.get_temporal_coverage_range() == "? - 2025-07-09"


def test_computed_get_temporal_coverage_range_empty():
    base = DatasetComputedColumns(
        {"temporal_coverage": {}},
        prefix="test",
    )

    assert base.get_temporal_coverage_range() is None


def test_computed_get_spatial_coordinates():
    geom = {
        "coordinates": [
            [
                [
                    [2.0629141330719, 46.8040313720703],
                    [7.18588781356812, 46.8040313720703],
                    [7.18588781356812, 44.1153793334961],
                    [2.0629141330719, 44.1153793334961],
                    [2.0629141330719, 46.8040313720703],
                ]
            ]
        ],
        "type": "MultiPolygon",
    }
    base = DatasetComputedColumns({"spatial": {"geom": geom}}, prefix="test")

    assert base.get_spatial_coordinates() == repr(geom["coordinates"])


def test_computed_get_spatial_coordinates_empty():
    geom = {"coordinates": [], "type": "MultiPolygon"}
    base = DatasetComputedColumns({"spatial": {"geom": geom}}, prefix="test")

    assert base.get_spatial_coordinates() is None


def test_computed_get_spatial_coordinates_missing():
    base = DatasetComputedColumns({"spatial": {"geom": {}}}, prefix="test")

    assert base.get_spatial_coordinates() is None


def test_computed_get_spatial_coordinates_too_long():
    geom = {
        "coordinates": [
            [
                [
                    [2.0629141330719, 46.8040313720703],
                    [7.18588781356812, 46.8040313720703],
                    [7.18588781356812, 44.1153793334961],
                    [2.0629141330719, 44.1153793334961],
                    [2.0629141330719, 46.8040313720703],
                ]
                * 20
            ]
        ],
        "type": "MultiPolygon",
    }
    base = DatasetComputedColumns({"spatial": {"geom": geom}}, prefix="test")

    coords = base.get_spatial_coordinates()
    assert coords is not None
    assert len(coords) <= DatasetComputedColumns.SPATIAL_COORDINATES_MAX_LENGTH


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
    "length,expected_bin,expected_label",
    upper_bounds_generator(
        DatasetComputedColumns.DESCRIPTION_UPPER_BOUNDS.values, lambda v: 1, True
    ),
)
def test_computed_get_description_bin(length, expected_bin, expected_label):
    bin = DatasetComputedColumns.get_bin(length, DatasetComputedColumns.DESCRIPTION_UPPER_BOUNDS)
    assert bin.bin == expected_bin
    assert bin.label == expected_label


def test_computed_get_quality_score():
    base = DatasetComputedColumns({"quality": {"score": 0.2}}, prefix="test")
    assert base.get_quality_score() == 0.2


@pytest.mark.parametrize(
    "score,expected_bin,expected_label",
    upper_bounds_generator(
        DatasetComputedColumns.QUALITY_SCORE_UPPER_BOUNDS.values, lambda v: ulp(v), False
    ),
)
def test_computed_get_quality_score_bin(score, expected_bin, expected_label):
    bin = DatasetComputedColumns.get_bin(score, DatasetComputedColumns.QUALITY_SCORE_UPPER_BOUNDS)
    assert bin.bin == expected_bin
    assert bin.label == expected_label


def test_computed_get_first_contact_point():
    base = DatasetComputedColumns(
        {
            "contact_points": [
                {"name": "toto", "email": "toto@example.com"},
                {"name": "titi", "email": "titi@example.com"},
            ]
        },
        prefix="test",
    )
    assert base.get_first_contact_point() == ContactPoint(name="toto", email="toto@example.com")


def test_computed_get_first_contact_point_name_only():
    base = DatasetComputedColumns({"contact_points": [{"name": "toto"}]}, prefix="test")
    assert base.get_first_contact_point() == ContactPoint(name="toto", email=None)


def test_computed_get_first_contact_point_email_only():
    base = DatasetComputedColumns(
        {"contact_points": [{"email": "toto@example.com"}]}, prefix="test"
    )
    assert base.get_first_contact_point() == ContactPoint(name=None, email="toto@example.com")


def test_computed_get_first_contact_point_empty():
    base = DatasetComputedColumns({"contact_points": []}, prefix="test")
    assert base.get_first_contact_point() is None


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
        "schema__exists": False,
    }

    assert actual | expected == actual  # type: ignore


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({"schema": None}, False),
        ({"schema": {"name": "foo", "url": None, "version": None}}, True),
        ({"schema": {"name": None, "url": "http://example.com/foo", "version": None}}, True),
        ({"schema": {"name": None, "url": None, "version": None}}, False),
    ],
)
def test_resource_schema(payload, expected):
    # using defaultdict so we don't have to specify all non-relevant but required resource keys
    default_payload = defaultdict(lambda: None, payload)
    actual = ResourceComputedColumns(default_payload).get_indicators()
    assert actual["schema__exists"] == expected


@pytest.mark.parametrize(
    "payload,expected",
    [
        ({}, False),
        ({"extras": None}, False),
        ({"extras": {}}, False),
        ({"extras": {"check:available": None}}, False),
        ({"extras": {"check:available": False}}, False),
        ({"extras": {"check:available": True}}, True),
    ],
)
def test_resource_available(payload, expected):
    # using defaultdict so we don't have to specify all non-relevant but required resource keys
    default_payload = defaultdict(lambda: None, {"id": "test", **payload})
    actual = Resource.from_payload(default_payload, "test")
    assert actual.available == expected


@pytest.mark.parametrize(
    "fixture_payload", ["bouquet_payload_ok.json"], indirect=["fixture_payload"]
)
def test_bouquet_theme(fixture_payload):
    bouquet = Bouquet.from_payload(
        fixture_payload,
        themes={"ecospheres-theme-mieux-se-deplacer": "Mieux se déplacer"},
    )
    assert bouquet.theme == "Mieux se déplacer"
