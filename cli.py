import os
import sys
import traceback
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import date, timedelta
from threading import Lock
from typing import Callable, NamedTuple

import requests
import sentry_sdk
from minicli import cli, run, wrap
from progressist import ProgressBar
from sqlalchemy import Select, and_, create_engine, func, inspect, select, text, update
from sqlalchemy.orm import scoped_session, sessionmaker

from alembic import command
from alembic.config import Config
from config import get_config_value, get_front_config
from metrics import add_metric, compute_quality_score, get_datagouvfr_metrics
from models import (
    Base,
    Bouquet,
    Dataset,
    DatasetBouquet,
    DatasetComputedColumns,
    DatasetMetric,
    EcospheresUniverseOrganization,
    Organization,
    Resource,
    Stats,
)
from utils import iter_rel, upsert

if sentry_dsn := os.getenv("SENTRY_DSN"):
    sentry_sdk.init(dsn=sentry_dsn)


class Task(NamedTuple):
    future: Future
    dataset: dict


class App:
    session: scoped_session
    org_lock: Lock

    def __init__(self):
        self.org_lock = Lock()


app = App()


def load_es_universe_organizations(env: str) -> list[EcospheresUniverseOrganization]:
    r = requests.get(get_config_value(env, "org_api"))
    r.raise_for_status()
    return [EcospheresUniverseOrganization.from_payload(o) for o in r.json()]


def load_organization(env: str, organization_id: str, refresh: bool = False) -> Organization | None:
    prefix = get_config_value(env, "prefix")
    url = f"https://{prefix}.data.gouv.fr/api/1/organizations/{organization_id}/"
    with app.org_lock:
        organization = (
            app.session.query(Organization).filter_by(organization_id=organization_id).first()
        )
        if not organization or refresh:
            r = requests.get(url)
            if not r.ok:
                if r.status_code == 410 or r.status_code == 404:
                    # TODO: delete from db?
                    print(f"Warning: organization {organization_id} has been deleted")
                    return
                else:
                    r.raise_for_status()
            org_db = Organization.from_payload(r.json())
            organization = upsert(app.session, org_db, organization)
        return organization


def _load_datagouvfr_metrics_batch(
    url: str, query: Select, id_field: str, update_fn: Callable, batch_size: int = 50
):
    total = app.session.scalar(select(func.count("*")).select_from(query.subquery()))
    bar = ProgressBar(total=total)
    result = app.session.execute(query).yield_per(batch_size)
    for batch in result.partitions():
        items = [row[0] for row in batch]
        metrics = get_datagouvfr_metrics(
            url,
            {
                f"{id_field}__in": ",".join(getattr(item, id_field) for item in items),
                "page_size": batch_size,
            },
        )
        for item in items:
            bar.update()
            metrics_data = next(
                (m for m in metrics if m[id_field] == getattr(item, id_field)), None
            )
            if not metrics_data:
                continue
            update_fn(item, metrics_data)
            app.session.add(item)
        try:
            app.session.commit()
        except Exception as e:
            app.session.rollback()
            print(f"Error updating batch: {e}")


@cli
def load_datagouvfr_metrics(env: str = "demo"):
    metrics_url = get_config_value(env, "metrics_api_url")
    if not metrics_url:
        print("No metrics API URL configured.")
        return

    # those metrics are always associated to the first of the month for data of last month
    at = date.today().replace(day=1)

    def handle_dataset(dataset: Dataset, metrics_data: dict):
        if monthly_visit := metrics_data.get("monthly_visit"):
            add_metric(
                app.session,
                "nb_visits_last_month",
                monthly_visit,
                at=at,
                dataset=dataset.dataset_id,
                metric_model=DatasetMetric,
            )
        if monthly_download_resource := metrics_data.get("monthly_download_resource"):
            add_metric(
                app.session,
                "nb_downloads_resources_last_month",
                monthly_download_resource,
                at=at,
                dataset=dataset.dataset_id,
                metric_model=DatasetMetric,
            )

    def handle_organization(org: Organization, metrics_data: dict):
        if monthly_visit_dataset := metrics_data.get("monthly_visit_dataset"):
            add_metric(
                app.session,
                "nb_visits_datasets_last_month",
                monthly_visit_dataset,
                at=at,
                organization=org.organization_id,
            )
        if monthly_download_resource := metrics_data.get("monthly_download_resource"):
            add_metric(
                app.session,
                "nb_downloads_resources_last_month",
                monthly_download_resource,
                at=at,
                organization=org.organization_id,
            )

    print("Loading metrics from data.gouv.fr for datasets...")
    datasets = select(Dataset).where(~Dataset.deleted)
    _load_datagouvfr_metrics_batch(
        f"{metrics_url}/datasets/data/", datasets, "dataset_id", handle_dataset
    )

    print("Loading metrics from data.gouv.fr for organizations...")
    organizations = select(Organization)
    _load_datagouvfr_metrics_batch(
        f"{metrics_url}/organizations/data/", organizations, "organization_id", handle_organization
    )


@cli
def update_organizations(env: str = "demo"):
    """Refresh and complement organizations"""
    print("Updating organizations...")
    organizations = app.session.query(Organization).all()
    custom_organizations = load_es_universe_organizations(env)
    for organization in organizations:
        fresh_organization = load_organization(env, organization.organization_id, refresh=True)
        if not fresh_organization:
            continue
        custom_organization = next(
            (o for o in custom_organizations if o.id == fresh_organization.organization_id), None
        )
        if custom_organization:
            fresh_organization.type = custom_organization.type
            app.session.add(fresh_organization)
        else:
            print("Skipping organization", fresh_organization.organization_id)
    app.session.commit()


@cli
def load_bouquets(env: str = "demo", include_private: bool = False):
    prefix = get_config_value(env, "prefix")

    # build a pallatable list of themes from remote config
    page_config = get_front_config(env)["pages"]["bouquets"]
    raw_themes = next((f for f in page_config["filters"] if f["id"] == "theme"), {"values": []})
    themes = {
        f"{page_config['tag_prefix']}-theme-{t['id']}": t["name"] for t in raw_themes["values"]
    }

    app.session.execute(text("DELETE FROM datasets_bouquets"))
    app.session.commit()

    # pre-set deleted, will be overwritten by actual upsert
    stmt = update(Bouquet).values(deleted=True)
    app.session.execute(stmt)
    app.session.commit()

    universe_name = get_config_value(env, "universe_name")
    url = f"https://{prefix}.data.gouv.fr/api/2/topics/?tag={universe_name}"
    if include_private:
        url = f"{url}&include_private=yes"

    for bouquet in iter_rel(
        {
            "href": url,
        }
    ):
        existing = app.session.query(Bouquet).filter_by(bouquet_id=bouquet["id"]).first()
        bouquet_obj = Bouquet.from_payload(bouquet, themes)
        bouquet_obj = upsert(app.session, bouquet_obj, existing)
        for dataset in iter_rel(bouquet["datasets"], quiet=True):
            dataset_obj = app.session.query(Dataset).filter_by(dataset_id=dataset["id"]).first()
            if dataset_obj:
                bouquet_obj.datasets.append(dataset_obj)
        app.session.commit()


def process_dataset(env: str, d: dict, licenses: list, skip_related: bool) -> None:
    """Process a single dataset and its resources"""
    prefix = get_config_value(env, "prefix")
    try:
        if organization_id := (d.get("organization") or {}).get("id"):
            load_organization(env, organization_id)

        dataset_obj = Dataset.from_payload(d, prefix, licenses)
        existing = app.session.query(Dataset).filter_by(dataset_id=dataset_obj.dataset_id).first()
        upsert(app.session, dataset_obj, existing)

        if not skip_related:
            for r in iter_rel(d["resources"], quiet=True):
                resource_obj = Resource.from_payload(r, dataset_obj.dataset_id)
                app.session.add(resource_obj)
            app.session.commit()
    except Exception as e:
        app.session.rollback()
        if sentry_dsn:
            sentry_sdk.capture_exception(e)
        raise e


@cli
def load(
    env: str = "demo",
    skip_related: bool = False,
    skip_metrics: bool = False,
    skip_stats: bool = False,
    max_workers: int = 4,
):
    """
    Load objects from our universe into the database:
    - datasets
    - organizations
    - resources (related)
    - bouquets (related)
    - organizations (related)

    Also compute associated metrics and load stats from Matomo.
    """
    prefix = get_config_value(env, "prefix")
    topic_slug = get_config_value(env, "topic_slug")
    request_topic = requests.get(f"https://{prefix}.data.gouv.fr/api/2/topics/{topic_slug}/")
    request_topic.raise_for_status()
    topic = request_topic.json()

    request_licenses = requests.get(f"https://{prefix}.data.gouv.fr/api/1/datasets/licenses/")
    request_licenses.raise_for_status()
    licenses = request_licenses.json()

    # pre-set deleted, will be overwritten by actual upsert
    stmt = update(Dataset).values(deleted=True)
    app.session.execute(stmt)
    app.session.commit()

    if not skip_related:
        app.session.execute(text("DELETE FROM resources"))
        app.session.commit()

    # Create a thread pool for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        tasks = []

        for dataset in iter_rel(topic["datasets"], page_size=200):
            future = executor.submit(
                process_dataset,
                env,
                dataset,
                licenses,
                skip_related=skip_related,
            )
            tasks.append(Task(future, dataset))

        for task in tasks:
            try:
                task.future.result()
            except Exception as e:
                print(f"Failed to process dataset {task.dataset['id']}: {str(e)}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    if not skip_related:
        update_organizations(env=env)
        load_bouquets(env=env, include_private=True)

    if not skip_metrics:
        # we're loading metrics from last month, only run on the second of the month
        if date.today().day == 2:
            load_datagouvfr_metrics(env=env)
        compute_metrics(env=env)

    if not skip_stats:
        load_stats(env=env)


@cli
def compute_metrics(env: str = "demo"):
    """
    Fill the time-series metrics table with today's data
    """
    print("Computing metrics...")

    query = (
        select(Dataset.organization)
        .distinct()
        .where(and_(~Dataset.deleted, Dataset.organization.is_not(None)))
    )
    org_ids = app.session.execute(query).scalars().all()
    add_metric(app.session, "nb_organizations", len(org_ids))

    agg = defaultdict(int)

    for org_id in org_ids:
        nb_datasets = (
            app.session.query(Dataset).filter_by(organization=org_id, deleted=False).count()
        )
        add_metric(app.session, "nb_datasets", nb_datasets, organization=org_id)
        agg["nb_datasets"] += nb_datasets

        # average quality score per organization
        add_metric(
            app.session,
            "avg_quality__score",
            compute_quality_score(app.session, org_id),
            organization=org_id,
        )

        for indicator in DatasetComputedColumns.indicators:
            field = indicator["field"]
            query = {
                "deleted": False,
                f"has_{field}": True,
                "organization": org_id,
            }
            measurement = f"nb_{field}"
            value = app.session.query(Dataset).filter_by(**query).count()
            add_metric(app.session, measurement, value, organization=org_id)
            agg[measurement] += value

    for agg_key, agg_value in agg.items():
        add_metric(app.session, agg_key, agg_value)

    # global average quality score
    add_metric(app.session, "avg_quality__score", compute_quality_score(app.session))

    # nb of associations bouquet <-> dataset from universe
    nb_datasets_bouquets = app.session.query(DatasetBouquet).count()
    add_metric(app.session, "nb_datasets_from_universe_in_bouquets", nb_datasets_bouquets)

    bouquets = app.session.query(Bouquet)
    add_metric(app.session, "nb_bouquets", bouquets.filter_by(deleted=False).count())
    add_metric(
        app.session, "nb_bouquets_public", bouquets.filter_by(deleted=False, private=False).count()
    )
    # nb_datasets_in_bouquets
    add_metric(
        app.session,
        "nb_datasets_in_bouquets",
        sum(b.nb_datasets for b in bouquets.filter_by(deleted=False)),
    )
    add_metric(
        app.session,
        "nb_datasets_in_bouquets_public",
        sum(b.nb_datasets for b in bouquets.filter_by(deleted=False, private=False)),
    )
    # nb_datasets_external_in_bouquets
    add_metric(
        app.session,
        "nb_datasets_external_in_bouquets",
        sum(b.nb_datasets_external for b in bouquets.filter_by(deleted=False)),
    )
    add_metric(
        app.session,
        "nb_datasets_external_in_bouquets_public",
        sum(b.nb_datasets_external for b in bouquets.filter_by(deleted=False, private=False)),
    )
    # nb_factors_in_bouquets
    add_metric(
        app.session,
        "nb_factors_in_bouquets",
        sum(b.nb_factors for b in bouquets.filter_by(deleted=False)),
    )
    add_metric(
        app.session,
        "nb_factors_in_bouquets_public",
        sum(b.nb_factors for b in bouquets.filter_by(deleted=False, private=False)),
    )
    # nb_factors_missing_in_bouquets
    add_metric(
        app.session,
        "nb_factors_missing_in_bouquets",
        sum(b.nb_factors_missing for b in bouquets.filter_by(deleted=False)),
    )
    add_metric(
        app.session,
        "nb_factors_missing_in_bouquets_public",
        sum(b.nb_factors_missing for b in bouquets.filter_by(deleted=False, private=False)),
    )
    # nb_factors_not_available_in_bouquets
    add_metric(
        app.session,
        "nb_factors_not_available_in_bouquets",
        sum(b.nb_factors_not_available for b in bouquets.filter_by(deleted=False)),
    )
    add_metric(
        app.session,
        "nb_factors_not_available_in_bouquets_public",
        sum(b.nb_factors_not_available for b in bouquets.filter_by(deleted=False, private=False)),
    )


@cli
def load_stats_history(env: str = "demo", since: str = "2024-04-02"):
    parsed_since = date.fromisoformat(since)
    print(f"Loading stats history since {since}...")
    today = date.today()
    for d in range((today - parsed_since).days):
        current_date = parsed_since + timedelta(d)
        load_stats(env=env, day=current_date.isoformat())


@cli
def load_stats(env: str = "demo", day: str | None = None):
    """
    Upsert the stats table from Matomo
    """
    # defaults to yesterday
    parsed_day = date.fromisoformat(day) if day else date.today() - timedelta(days=1)
    print(f"Loading stats for {parsed_day.isoformat()}...")

    stats_url = get_config_value(env, "stats_url")
    stats_site_id = get_config_value(env, "stats_site_id")
    stats_token = get_config_value(env, "stats_token")

    if not stats_url or not stats_site_id or not stats_token:
        print("Skipping stats loading: missing config value(s)")
        return

    common_args = {
        "module": "API",
        "idSite": stats_site_id,
        "token_auth": stats_token,
        "period": "day",
        "date": parsed_day.isoformat(),
        "format": "JSON",
    }

    def fetch(method: str) -> dict:
        r = requests.post(
            stats_url,
            data={
                **common_args,
                "method": method,
            },
        )
        r.raise_for_status()
        return r.json()

    data = {}
    methods = ["VisitsSummary.get", "Actions.get", "VisitFrequency.get"]
    for method in methods:
        data |= fetch(method)

    columns = [column.key for column in inspect(Stats).attrs if column.key != "id"]
    db_data = {k: v for k, v in data.items() if k in columns}
    db_data["date"] = parsed_day
    if "bounce_rate" in db_data:
        # 39% -> 0.39
        db_data["bounce_rate"] = float(db_data["bounce_rate"].rstrip("%")) / 100

    existing = app.session.query(Stats).filter_by(date=parsed_day).first()
    upsert(app.session, Stats(**db_data), existing)


@cli
def init_db(env: str = "demo"):
    """Create the tables in the env database from current schema"""
    engine = app.session.get_bind()
    Base.metadata.create_all(engine)
    # mark current schema as up-to-date re alembic
    os.environ["ALEMBIC_ENV"] = env
    alembic_cfg = Config("alembic.ini")
    command.stamp(alembic_cfg, "head")


@wrap
def connect(env: str):
    """Create a wrapped session for cli commands in App.session"""
    print(f"Working on env {env!r}")
    dsn = get_config_value(env, "dsn")
    engine = create_engine(dsn)
    connection = engine.connect()
    app.session = scoped_session(sessionmaker(autoflush=True, bind=engine))
    yield
    app.session.close()
    connection.close()


if __name__ == "__main__":
    # env is a global parameter that can be overloaded via --env
    # every cli command has access to it, but does not _need_ to declare it
    run(env="demo")
