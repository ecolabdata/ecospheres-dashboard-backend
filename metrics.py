from datetime import date
from typing import Type, TypeAlias

import requests
from sqlalchemy import text
from sqlalchemy.orm import scoped_session

from models import DatasetMetric, Metric
from utils import upsert

MetricModel: TypeAlias = Type[Metric] | Type[DatasetMetric]


def add_metric(
    session: scoped_session,
    measurement: str,
    value: float | None,
    metric_model: MetricModel = Metric,
    at: date = date.today(),
    **kwargs,
):
    metric_obj = metric_model(date=at, measurement=measurement, value=value, **kwargs)
    existing = (
        session.query(metric_model).filter_by(date=at, measurement=measurement, **kwargs).first()
    )
    return upsert(session, metric_obj, existing)


def quality_score_query(organization: str | None = None) -> tuple[str, dict]:
    kwargs = {}
    q = "SELECT AVG((quality->>'score')::numeric) AS mean_score FROM catalog"
    if organization:
        q = f"{q} WHERE organization = :org"
        kwargs["org"] = organization
    return q, kwargs


def compute_quality_score(session: scoped_session, organization: str | None = None) -> float | None:
    q, kwargs = quality_score_query(organization)
    return session.execute(text(q), kwargs).scalar()


def get_datagouvfr_metrics(url: str, params: dict) -> list:
    now = date.today()
    # metrics for last full month
    metrics_month = f"{now.year}-{str(now.month - 1).zfill(2)}"
    params["metric_month__exact"] = metrics_month
    r = requests.get(url, params=params)
    if r.ok:
        return r.json()["data"]
    return []
