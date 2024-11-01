import math
import time
from typing import TypeAlias, TypeVar

import requests
from sqlalchemy.orm import scoped_session

from models import Rel
from models_new import Bouquet, Dataset, DatasetBouquet, Organization, Resource

Model: TypeAlias = Bouquet | Dataset | DatasetBouquet | Organization | Resource
T = TypeVar("T", bound=Model)

# def upsert(session: scoped_session, data: DeclarativeBaseWithId, cmp_clause: dict):
#     existing = session.query(type(data)).filter_by(**cmp_clause).first()
#     if existing:
#         data.id = existing.id
#         session.merge(data)
#     else:
#         session.add(data)
#     session.commit()


def upsert(session: scoped_session, new: T, existing: T | None) -> T:
    if existing:
        new.id = existing.id
        session.merge(new)
        session.flush()
        return existing
    else:
        session.add(new)
        session.flush()
        return new


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
