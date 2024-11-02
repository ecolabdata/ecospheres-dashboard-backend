from metrics import compute_quality_score, quality_score_query


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
