import logging
from datetime import date, timedelta
from typing import Type, TypeAlias

import requests
from requests.sessions import Session
from sqlalchemy import text
from sqlalchemy.orm import scoped_session

from models import DatasetMetric, Metric
from utils import upsert

MetricModel: TypeAlias = Type[Metric] | Type[DatasetMetric]

log = logging.getLogger(__name__)


def previous_month(d: date) -> date:
    """First day of the month before `d`'s month"""
    return (d.replace(day=1) - timedelta(days=1)).replace(day=1)


def next_month(d: date) -> date:
    """First day of the month after `d`'s month"""
    return (d.replace(day=28) + timedelta(days=4)).replace(day=1)


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


def get_datagouvfr_metrics(
    url: str, params: dict, session: Session | None = None, month: date | None = None
) -> list:
    """Fetch metrics for `month` (any day of it), defaults to last full month"""
    s = session or requests
    month = month or previous_month(date.today())
    params["metric_month__exact"] = month.strftime("%Y-%m")
    r = s.get(url, params=params)
    if r.ok:
        return r.json()["data"]
    # error level so that Sentry's logging integration reports it as an event
    log.error(
        "Failed to fetch metrics for %s from %s: HTTP %s", f"{month:%Y-%m}", url, r.status_code
    )
    return []
