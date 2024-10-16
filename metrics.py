from db import query


def compute_quality_score(organization: str | None = None) -> float | None:
    kwargs = {}
    q = "SELECT AVG((quality->>'score')::numeric) AS mean_score FROM catalog"
    if organization:
        q = f"{q} WHERE organization = :org"
        kwargs["org"] = organization
    avg_quality__score = next(query(q, **kwargs))
    return avg_quality__score["mean_score"] if avg_quality__score else None
