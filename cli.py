import math
import time

from datetime import date
from collections import defaultdict

import requests

from minicli import cli, run
from sqlalchemy.types import Float

from db import get_table, query, get_tables
from models import Organization, Dataset, DatasetBouquet, Rel, Resource, Bouquet


def iter_rel(rel: Rel, quiet: bool = False):
    current_url = rel["href"]
    if not quiet:
        print(f"Fetching {current_url}...")
    while current_url is not None:
        while True:
            r = requests.get(current_url)
            if not r.ok:
                if r.status_code == 429:
                    print("429 hit, waiting a bit")
                    time.sleep(10)
                    continue
                else:
                    r.raise_for_status()
            break
        payload = r.json()
        total_pages = math.ceil(payload["total"] / payload["page_size"])
        if not quiet:
            print(f"Handling page {payload['page']}/{total_pages}")
        current_url = payload["next_page"]
        for d in payload["data"]:
            yield d


@cli
def load_organizations(env: str = "demo", refresh: bool = False):
    prefix = "www" if env == "prod" else env
    url = f"https://{prefix}.data.gouv.fr/api/1/organizations"
    catalog = get_table("catalog")
    organizations = get_table("organizations")
    org_ids = set(
        [
            d["organization"]
            for d in catalog.all()
            if d is not None and d["organization"] is not None
        ]
    )
    print(f"Handling {len(org_ids)} organizations from catalog...")
    for org_id in org_ids:
        print(org_id)
        existing = organizations.find_one(organization_id=org_id)
        if not existing or refresh:
            r = requests.get(f"{url}/{org_id}/")
            r.raise_for_status()
            organizations.upsert(Organization.from_payload(r.json()), ["organization_id"])


@cli
def load_bouquets(
    env: str = "demo", universe_name: str = "ecospheres", include_private: bool = False
):
    prefix = "www" if env == "prod" else env

    catalog = get_table("catalog")

    datasets_bouquets = get_table("datasets_bouquets")
    if datasets_bouquets.exists:
        datasets_bouquets.drop()

    bouquets = get_table("bouquets")
    if bouquets.exists:
        # pre-set deleted, will be overwritten by actual upsert
        query("UPDATE bouquets SET deleted = TRUE")

    url = f"https://{prefix}.data.gouv.fr/api/2/topics/?tag={universe_name}"
    if include_private:
        url = f"{url}&include_private=yes"

    for bouquet in iter_rel({
        "href": url,
    }):
        bouquets.upsert(Bouquet.from_payload(bouquet), ["bouquet_id"])
        datasets_bouquets.insert_many(DatasetBouquet.from_payload(bouquet, catalog))


@cli
def load(
    env: str = "demo",
    topic_slug: str = "univers-ecospheres",
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
    prefix = "www" if env == "prod" else env
    r = requests.get(f"https://{prefix}.data.gouv.fr/api/2/topics/{topic_slug}/")
    r.raise_for_status()
    topic = r.json()

    table = get_table("catalog")
    if table.exists:
        # pre-set deleted, will be overwritten by actual upsert
        query("UPDATE catalog SET deleted = TRUE")

    resources_table = get_table("resources")
    if "resources" in get_tables() and not skip_related:
        resources_table.drop()

    for d in iter_rel(topic["datasets"]):
        dataset = Dataset(d)
        table.upsert(dataset.to_row(), ["dataset_id"], types=Dataset.col_types())
        if not skip_related:
            for r in iter_rel(d["resources"], quiet=True):
                resources_table.upsert(Resource.from_payload(d["id"], r), ["resource_id"])

    if not skip_related:
        load_organizations()
        load_bouquets(include_private=True)

    if not skip_metrics:
        compute_metrics()


@cli
def compute_metrics():
    """
    Fill the time-series metrics table with today's data
    """
    catalog = get_table("catalog")
    metrics = get_table("metrics")
    at = date.today()

    def add_metric(
        measurement: str,
        value: int,
        organization: str | None = None,
    ):
        metrics.upsert({
            "date": at,
            "measurement": measurement,
            "value": value,
            "organization": organization,
        }, ["date", "measurement", "organization"], types={"value": Float})

    organizations = [r["organization"] for r in catalog.distinct("organization", deleted=False)]
    add_metric("nb_organizations", len(organizations))

    agg = defaultdict(int)

    for org in organizations:
        nb_datasets = catalog.count(deleted=False, organization=org)
        add_metric("nb_datasets", nb_datasets, organization=org)
        agg["nb_datasets"] += nb_datasets

        for indicator in Dataset.indicators:
            query = {
                "deleted": False,
                f"has_{indicator['id']}": True,
                "organization": org,
            }
            measurement = f"nb_{indicator['id']}"
            value = catalog.count(**query)
            add_metric(measurement, value, organization=org)
            agg[measurement] += value

    for agg_key, agg_value in agg.items():
        add_metric(agg_key, agg_value)

    datasets_bouquets = get_table("datasets_bouquets")
    # nb of associations bouquet <-> dataset from universe
    add_metric("nb_datasets_from_universe_in_bouquets", datasets_bouquets.count())

    bouquets = get_table("bouquets")
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


if __name__ == "__main__":
    run()
