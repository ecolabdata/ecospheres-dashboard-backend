import math
import time

import requests

from minicli import cli, run

from db import get_table, query, get_tables
from models import Organization, Dataset, Bouquet, Rel


def iter_rel(rel: Rel):
    current_url = rel["href"]
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
    table = get_table("datasets_bouquets")
    if "datasets_bouquets" in get_tables():
        table.drop()

    url = f"https://{prefix}.data.gouv.fr/api/2/topics/?tag={universe_name}"
    if include_private:
        url = f"{url}&include_private=yes"

    for bouquet in iter_rel({
        "href": url,
    }):
        rows = Bouquet.from_payload(bouquet)
        for row in rows:
            dataset = catalog.find_one(dataset_id=row["dataset_id"])
            if dataset:
                dataset.pop("id")
                table.insert({
                    "bouquet_id": row["bouquet_id"],
                    "bouquet_name": row["name"],
                    **dataset
                })


@cli
def load(
    env: str = "demo",
    topic_slug: str = "univers-ecospheres",
    skip_related: bool = False,
):
    prefix = "www" if env == "prod" else env
    r = requests.get(f"https://{prefix}.data.gouv.fr/api/2/topics/{topic_slug}/")
    r.raise_for_status()
    topic = r.json()

    if "catalog" in get_tables():
        # pre-set deleted, will be overwritten by actual upsert
        query("UPDATE catalog SET deleted = TRUE")
    table = get_table("catalog")

    for d in iter_rel(topic["datasets"]):
        table.upsert(Dataset.from_payload(d), ["dataset_id"])

    if not skip_related:
        load_organizations()
        load_bouquets(include_private=True)


if __name__ == "__main__":
    run()
