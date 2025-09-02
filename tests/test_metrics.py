from datetime import date
from unittest.mock import patch

from metrics import compute_quality_score, get_datagouvfr_metrics, quality_score_query


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


@patch("metrics.date")
def test_get_datagouvfr_metrics(mock_date, mock_requests):
    url = "https://example.com/api"
    mock_response = {
        "data": [
            {
                "__id": 50271644,
                "dataset_id": "5c4ae55a634f4117716d5656",
                "metric_month": "2025-05",
                "monthly_visit": 59994,
                "monthly_download_resource": 9342,
            }
        ],
        "links": {"next": None, "prev": None},
        "meta": {"page": 1, "page_size": 20, "total": 1},
    }
    mock_requests.get(url, json=mock_response)
    mock_date.today.return_value = date(2025, 6, 1)
    result = get_datagouvfr_metrics(url, {})
    assert result == [
        {
            "__id": 50271644,
            "dataset_id": "5c4ae55a634f4117716d5656",
            "metric_month": "2025-05",
            "monthly_visit": 59994,
            "monthly_download_resource": 9342,
        }
    ]
    assert mock_requests.request_history[0].qs == {"metric_month__exact": ["2025-05"]}


def test_get_datagouvfr_metrics_not_found(mock_requests):
    url = "https://example.com/api"
    mock_requests.get(url, status_code=404)
    result = get_datagouvfr_metrics(url, {})
    assert result == []
