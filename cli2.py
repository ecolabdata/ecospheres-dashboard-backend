import os

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models_new import Dataset


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

    dataset = requests.get(
        "https://demo.data.gouv.fr/api/2/datasets/etat-davancement-des-eoliennes-dans-le-departement-de-la-marne-1/"
    ).json()
    licenses = requests.get("https://demo.data.gouv.fr/api/1/datasets/licenses/").json()

    ds_org = Dataset.from_payload(dataset, "demo", licenses)

    # why the hell does SqlAlchemy not have an upsert??
    existing = session.query(Dataset).filter_by(dataset_id=ds_org.dataset_id).first()
    if not existing:
        print("Creating new dataset")
        session.add(ds_org)
    else:
        print("Updating existing dataset")
        ds_org.id = existing.id
        # FIXME: not updating anything?
        session.merge(ds_org)

    session.commit()


if __name__ == "__main__":
    main()
