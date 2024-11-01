import math
import os
import time

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models_new import Dataset, Resource


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


def main():
    # Get database URL from environment
    db_url = os.environ.get("DATABASE_URL_PROD")
    if not db_url:
        print("Error: DATABASE_URL_PROD environment variable not set")
        return

    # Create engine and session
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

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

    upsert_dataset(
        "https://demo.data.gouv.fr/api/2/datasets/etat-davancement-des-eoliennes-dans-le-departement-de-la-marne-1/"
    )
    upsert_dataset("https://demo.data.gouv.fr/api/2/datasets/66bd4fe809c8aa3c089b64a0/")
    upsert_dataset("https://demo.data.gouv.fr/api/2/datasets/repertoire-national-des-associations/")

    session.commit()


if __name__ == "__main__":
    main()
