from datetime import date

import pytest
import requests_mock

from metrics import compute_quality_score, get_datagouvfr_metrics, quality_score_query


@pytest.fixture
def mock_requests():
    with requests_mock.Mocker() as m:
        yield m


def test_compute_quality_score():
    class MockSession:
        def execute(self, *args, **kwargs):
            class MockExecute:
                def scalar(self):
                    return 0.5

            return MockExecute()

    assert compute_quality_score(MockSession()) == 0.5  # type: ignore


def test_quality_score_query():
    q, kwargs = quality_score_query()
    assert "mean_score" in q
    assert kwargs == {}

    o_q, o_kwargs = quality_score_query(organization="org")
    assert q in o_q
    assert "organization =" in o_q
    assert o_kwargs == {"org": "org"}


def test_get_datagouvfr_metrics(mock_requests):
    url = "https://example.com/api"
    now = date.today()
    metrics_month = f"{now.year}-{str(now.month - 1).zfill(2)}"
    mock_response = {
        "data": [
            {
                "__id": 50271644,
                "dataset_id": "5c4ae55a634f4117716d5656",
                "metric_month": metrics_month,
                "monthly_visit": 59994,
                "monthly_download_resource": 9342,
            }
        ],
        "links": {"next": None, "prev": None},
        "meta": {"page": 1, "page_size": 20, "total": 1},
    }
    mock_requests.get(url, json=mock_response)
    result = get_datagouvfr_metrics(url, {})
    assert result == mock_response["data"]
    assert mock_requests.request_history[0].qs == {"metric_month__exact": [metrics_month]}


def test_get_datagouvfr_metrics_not_found(mock_requests):
    url = "https://example.com/api"
    mock_requests.get(url, status_code=404)
    result = get_datagouvfr_metrics(url, {})
    assert result == []
