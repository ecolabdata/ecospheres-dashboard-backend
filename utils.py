import math
import re
import time
from collections.abc import Sequence
from typing import Any, Protocol, TypedDict

import requests
from sqlalchemy.orm import scoped_session


def no_value_dict(obj: Any) -> bool:
    return isinstance(obj, dict) and all(v is None for v in obj.values())


DEFAULT_EXCLUDE = (None,)
DEFAULT_JSON_EXCLUDE = (None, {}, no_value_dict)
DEFAULT_LIST_EXCLUDE = (None, [])
DEFAULT_STRING_EXCLUDE = (None, "")


def accept(element: Any, exclude: Sequence[Any] = DEFAULT_EXCLUDE) -> bool:
    """
    Return True if `element` is not in the `exclude` sequence, False otherwise.

    The `exclude` sequence can contain:
    - A value which should be excluded.
    - A callable taking `element` as a single parameter and returning True iff `element` should be excluded.
    """
    for item in exclude:
        if callable(item) and item(element):
            return False
        elif element == item:
            return False
    return True


class HasId(Protocol):
    id: Any


def upsert[T: HasId](
    session: scoped_session, new: T, existing: T | None, auto_commit: bool = True
) -> T:
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


def iter_rel(rel: Rel, quiet: bool = False, page_size: int | None = None):
    current_url = rel["href"]
    if page_size:
        current_url = re.sub(r"page_size=(?:[0-9]+)", f"page_size={page_size}", current_url)
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
