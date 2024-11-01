from sqlalchemy import text
from sqlalchemy.orm import scoped_session


def quality_score_query(organization: str | None = None) -> tuple[str, dict]:
    kwargs = {}
    q = "SELECT AVG((quality->>'score')::numeric) AS mean_score FROM catalog"
    if organization:
        q += " WHERE organization = :org"
        kwargs["org"] = organization
    return q, kwargs


def compute_quality_score(session: scoped_session, organization: str | None = None) -> float | None:
    q, kwargs = quality_score_query(organization)
    return session.execute(text(q), kwargs).scalar()
