from unittest.mock import patch

from metrics import compute_quality_score


def test_compute_quality_score():
    class MockDb:
        def query(self, *args, **kwargs):
            yield {"mean_score": 0.5}

    with patch("db.get", MockDb):
        assert compute_quality_score() == 0.5
