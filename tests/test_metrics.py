from unittest.mock import patch

from metrics import compute_quality_score, quality_score_query


def test_compute_quality_score():
    class MockDb:
        def query(self, *args, **kwargs):
            yield {"mean_score": 0.5}

    with patch("db.get", MockDb):
        assert compute_quality_score() == 0.5


def test_quality_score_query():
    q, kwargs = quality_score_query()
    assert "mean_score" in q
    assert kwargs == {}

    o_q, o_kwargs = quality_score_query(organization="org")
    assert q in o_q
    assert "organization =" in o_q
    assert o_kwargs == {"org": "org"}
