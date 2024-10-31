from db import query


def quality_score_query(organization: str | None = None) -> tuple[str, dict]:
    kwargs = {}
    q = "SELECT AVG((quality->>'score')::numeric) AS mean_score FROM catalog"
    if organization:
        q = f"{q} WHERE organization = :org"
        kwargs["org"] = organization
    return q, kwargs


def compute_quality_score(env: str, organization: str | None = None) -> float | None:
    q, kwargs = quality_score_query(organization)
    avg_quality__score = next(query(env, q, **kwargs))
    return avg_quality__score["mean_score"] if avg_quality__score else None
