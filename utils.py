import math
import time
from typing import TypeAlias, TypedDict, TypeVar

import requests
from sqlalchemy.orm import scoped_session

from models import Bouquet, Dataset, Metric, Organization, Resource, Stats

Model: TypeAlias = Bouquet | Dataset | Metric | Organization | Resource | Stats
T = TypeVar("T", bound=Model)


def upsert(session: scoped_session, new: T, existing: T | None, auto_commit: bool = True) -> T:
    if existing:
        new.id = existing.id
        session.merge(new)
        result = existing
    else:
        session.add(new)
        result = new
    # creates the id if needed
    session.flush()
    if auto_commit:
        session.commit()
    return result


class Rel(TypedDict):
    href: str


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
