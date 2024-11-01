from collections import defaultdict
from datetime import date

import requests
from minicli import cli, run, wrap
from sqlalchemy import create_engine, select
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.types import Float

from config import get_config_value
from db import get_table, get_tables, query
from metrics import compute_quality_score
from models import Bouquet, DatasetBouquet, Resource
from models_new import Dataset, Organization
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


@cli
def load_bouquets(env: str = "demo", include_private: bool = False):
    prefix = get_config_value(env, "prefix")
    catalog = get_table(env, "catalog")

    datasets_bouquets = get_table(env, "datasets_bouquets")
    if datasets_bouquets.exists:
        datasets_bouquets.drop()

    bouquets = get_table(env, "bouquets")
    if bouquets.exists:
        # pre-set deleted, will be overwritten by actual upsert
        query(env, "UPDATE bouquets SET deleted = TRUE")

    universe_name = get_config_value(env, "universe_name")
    url = f"https://{prefix}.data.gouv.fr/api/2/topics/?tag={universe_name}"
    if include_private:
        url = f"{url}&include_private=yes"

    for bouquet in iter_rel(
        {
            "href": url,
        }
    ):
        bouquets.upsert(Bouquet.from_payload(bouquet), ["bouquet_id"])
        datasets_bouquets.insert_many(DatasetBouquet.from_payload(bouquet, catalog))


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

    table = get_table(env, "catalog")
    if table.exists:
        # pre-set deleted, will be overwritten by actual upsert
        query(env, "UPDATE catalog SET deleted = TRUE")

    resources_table = get_table(env, "resources")
    if "resources" in get_tables(env) and not skip_related:
        resources_table.drop()

    for d in iter_rel(topic["datasets"]):
        dataset = Dataset(d, prefix, licenses)
        table.upsert(dataset.to_row(), ["dataset_id"], types=Dataset.col_types())

        if not skip_related:
            for r in iter_rel(d["resources"], quiet=True):
                resources_table.upsert(Resource.from_payload(d["id"], r), ["resource_id"])

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
    catalog = get_table(env, "catalog")
    metrics = get_table(env, "metrics")
    at = date.today()

    def add_metric(
        measurement: str,
        value: float | None,
        organization: str | None = None,
    ):
        metrics.upsert(
            {
                "date": at,
                "measurement": measurement,
                "value": value,
                "organization": organization,
            },
            ["date", "measurement", "organization"],
            types={"value": Float},
        )

    organizations = [r["organization"] for r in catalog.distinct("organization", deleted=False)]
    add_metric("nb_organizations", len(organizations))

    agg = defaultdict(int)

    for org in organizations:
        nb_datasets = catalog.count(deleted=False, organization=org)
        add_metric("nb_datasets", nb_datasets, organization=org)
        agg["nb_datasets"] += nb_datasets

        # average quality score per organization
        add_metric("avg_quality__score", compute_quality_score(env, org), organization=org)

        for indicator in Dataset.indicators:
            field = indicator["field"]
            query = {
                "deleted": False,
                f"has_{field}": True,
                "organization": org,
            }
            measurement = f"nb_{field}"
            value = catalog.count(**query)
            add_metric(measurement, value, organization=org)
            agg[measurement] += value

    for agg_key, agg_value in agg.items():
        add_metric(agg_key, agg_value)

    # global average quality score
    add_metric("avg_quality__score", compute_quality_score(env))

    datasets_bouquets = get_table(env, "datasets_bouquets")
    # nb of associations bouquet <-> dataset from universe
    add_metric("nb_datasets_from_universe_in_bouquets", datasets_bouquets.count())

    bouquets = get_table(env, "bouquets")
    add_metric("nb_bouquets", bouquets.count(deleted=False))
    add_metric("nb_bouquets_public", bouquets.count(private=False))
    add_metric(
        "nb_datasets_in_bouquets",
        sum(b["nb_datasets"] for b in bouquets.find(deleted=False) if b),
    )
    add_metric(
        "nb_factors_in_bouquets",
        sum(b["nb_factors"] for b in bouquets.find(deleted=False) if b),
    )


@wrap
def connect(env: str):
    """Create a wrapped session for cli commands in App.session"""
    dsn = get_config_value(env, "dsn")
    print(f"Connecting to {dsn}")
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
