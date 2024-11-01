import math
import os
import time

import requests
from minicli import cli, run
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models_new import Bouquet, Dataset, Organization, Resource


def iter_rel(rel, quiet: bool = False):
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


db_url = os.environ.get("DATABASE_URL_PROD")
if not db_url:
    raise ValueError("Error: DATABASE_URL_PROD environment variable not set")

# Create engine and session
engine = create_engine(db_url)
Session = sessionmaker(bind=engine)
session = Session()


@cli
def load():
    # Get database URL from environment
    licenses = requests.get("https://demo.data.gouv.fr/api/1/datasets/licenses/").json()

    def upsert_dataset(url: str):
        dataset = requests.get(url).json()
        print(dataset["id"], dataset["resources"])

        dataset_obj = Dataset.from_payload(dataset, "demo", licenses)

        # why the hell does SqlAlchemy not have an upsert??
        existing = session.query(Dataset).filter_by(dataset_id=dataset_obj.dataset_id).first()
        if not existing:
            print("Creating new dataset")
            session.add(dataset_obj)
        else:
            print("Updating existing dataset")
            dataset_obj.id = existing.id
            session.merge(dataset_obj)

        print("Handling resources")
        for resource in iter_rel(dataset["resources"], quiet=True):
            resource_db = Resource.from_payload(resource, dataset_obj.dataset_id)
            existing = (
                session.query(Resource).filter_by(resource_id=resource_db.resource_id).first()
            )
            if not existing:
                session.add(resource_db)
            else:
                resource_db.id = existing.id
                session.merge(resource_db)

        org = requests.get(dataset["organization"]["uri"]).json()
        print("Handling organization", org["id"])
        organization_db = Organization.from_payload(org)
        existing = (
            session.query(Organization)
            .filter_by(organization_id=organization_db.organization_id)
            .first()
        )
        if not existing:
            session.add(organization_db)
        else:
            organization_db.id = existing.id
            session.merge(organization_db)

    upsert_dataset(
        "https://demo.data.gouv.fr/api/2/datasets/etat-davancement-des-eoliennes-dans-le-departement-de-la-marne-1/"
    )
    upsert_dataset("https://demo.data.gouv.fr/api/2/datasets/66bd4fe809c8aa3c089b64a0/")
    upsert_dataset("https://demo.data.gouv.fr/api/2/datasets/repertoire-national-des-associations/")

    print("Handling bouquets")
    bouquet = requests.get("https://demo.data.gouv.fr/api/2/topics/itineraires-fraicheur/").json()
    print(bouquet["id"])
    bouquet_db = Bouquet.from_payload(bouquet)
    existing = session.query(Bouquet).filter_by(bouquet_id=bouquet_db.bouquet_id).first()
    if not existing:
        session.add(bouquet_db)
    else:
        bouquet_db.id = existing.id
        session.merge(bouquet_db)

    session.commit()


@cli
def read():
    organization = (
        session.query(Organization).filter_by(organization_id="534fff91a3a7292c64a77f53").first()
    )
    if organization:
        print(organization)
        print([d for d in organization.datasets])
        print("--")

    dataset = session.query(Dataset).filter_by(dataset_id="58e53811c751df03df38f42d").first()
    if dataset:
        print(dataset)
        print(dataset.resources)
        print("--")

    bouquet = session.query(Bouquet).filter_by(bouquet_id="66685a015941166af9e09640").first()
    if bouquet:
        print(bouquet)
        print(bouquet.organization)
        print(bouquet.datasets)


if __name__ == "__main__":
    run()
