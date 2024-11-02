import os
from collections import defaultdict
from datetime import date

import requests
from minicli import cli, run, wrap
from sqlalchemy import create_engine, select, text, update
from sqlalchemy.orm import scoped_session, sessionmaker

from alembic import command
from alembic.config import Config
from config import get_config_value
from metrics import compute_quality_score
from models import (
    Base,
    Bouquet,
    Dataset,
    DatasetBouquet,
    DatasetComputedColumns,
    Metric,
    Organization,
    Resource,
)
from utils import iter_rel, upsert


class App:
    session: scoped_session


app = App()


@cli
def load_organizations(env: str = "demo", refresh: bool = False):
    prefix = get_config_value(env, "prefix")
    url = f"https://{prefix}.data.gouv.fr/api/1/organizations"
    query = select(Dataset.organization).distinct()
    org_ids = app.session.execute(query).scalars().all()
    print(f"Handling {len(org_ids)} organizations from catalog...")
    for org_id in org_ids:
        print(org_id)
        existing = app.session.query(Organization).filter_by(organization_id=org_id).first()
        if not existing or refresh:
            r = requests.get(f"{url}/{org_id}/")
            if not r.ok:
                if r.status_code == 410:
                    # TODO: delete from db?
                    print(f"Warning: organization {org_id} has been deleted")
                    continue
                else:
                    r.raise_for_status()
            org_db = Organization.from_payload(r.json())
            upsert(app.session, org_db, existing)
            app.session.commit()


@cli
def load_bouquets(env: str = "demo", include_private: bool = False):
    prefix = get_config_value(env, "prefix")

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
        bouquet_obj = Bouquet.from_payload(bouquet)
        bouquet_obj = upsert(app.session, bouquet_obj, existing)
        for dataset in iter_rel(bouquet["datasets"], quiet=True):
            dataset_obj = app.session.query(Dataset).filter_by(dataset_id=dataset["id"]).first()
            if dataset_obj:
                bouquet_obj.datasets.append(dataset_obj)
        app.session.commit()


@cli
def load(
    env: str = "demo",
    skip_related: bool = False,
    skip_metrics: bool = False,
):
    """
    Load objects from our universe into the database:
    - datasets
    - resources (related)
    - organizations (related)
    - bouquets (related)

    And compute associated metrics.
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

    for d in iter_rel(topic["datasets"]):
        dataset_obj = Dataset.from_payload(d, prefix, licenses)
        existing = app.session.query(Dataset).filter_by(dataset_id=dataset_obj.dataset_id).first()
        upsert(app.session, dataset_obj, existing)
        app.session.commit()

        if not skip_related:
            for r in iter_rel(d["resources"], quiet=True):
                resource_obj = Resource.from_payload(r, dataset_obj.dataset_id)
                app.session.add(resource_obj)
            app.session.commit()

    if not skip_related:
        load_organizations(env=env)
        load_bouquets(env=env, include_private=True)

    if not skip_metrics:
        compute_metrics(env=env)


@cli
def compute_metrics(env: str = "demo"):
    """
    Fill the time-series metrics table with today's data
    """
    print("Computing metrics...")
    at = date.today()

    def add_metric(
        measurement: str,
        value: float | None,
        organization: str | None = None,
    ):
        metric_obj = Metric(
            date=at, measurement=measurement, value=value, organization=organization
        )
        existing = (
            app.session.query(Metric)
            .filter_by(date=at, measurement=measurement, organization=organization)
            .first()
        )
        upsert(app.session, metric_obj, existing)
        app.session.commit()

    query = select(Dataset.organization).distinct()
    org_ids = app.session.execute(query).scalars().all()
    add_metric("nb_organizations", len(org_ids))

    agg = defaultdict(int)

    for org_id in org_ids:
        nb_datasets = (
            app.session.query(Dataset).filter_by(organization=org_id, deleted=False).count()
        )
        add_metric("nb_datasets", nb_datasets, organization=org_id)
        agg["nb_datasets"] += nb_datasets

        # average quality score per organization
        add_metric(
            "avg_quality__score", compute_quality_score(app.session, org_id), organization=org_id
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
            add_metric(measurement, value, organization=org_id)
            agg[measurement] += value

    for agg_key, agg_value in agg.items():
        add_metric(agg_key, agg_value)

    # global average quality score
    add_metric("avg_quality__score", compute_quality_score(app.session))

    # nb of associations bouquet <-> dataset from universe
    nb_datasets_bouquets = app.session.query(DatasetBouquet).count()
    add_metric("nb_datasets_from_universe_in_bouquets", nb_datasets_bouquets)

    bouquets = app.session.query(Bouquet)
    add_metric("nb_bouquets", bouquets.filter_by(deleted=False).count())
    add_metric("nb_bouquets_public", bouquets.filter_by(deleted=False, private=False).count())
    add_metric(
        "nb_datasets_in_bouquets",
        sum(b.nb_datasets for b in bouquets.filter_by(deleted=False)),
    )
    add_metric(
        "nb_factors_in_bouquets",
        sum(b.nb_factors for b in bouquets.filter_by(deleted=False)),
    )


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
