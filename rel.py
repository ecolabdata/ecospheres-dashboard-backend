import math
import re
from typing import TypedDict

import requests
from requests.sessions import Session


class Rel(TypedDict):
    href: str


def iter_rel(
    rel: Rel,
    quiet: bool = False,
    page_size: int | None = None,
    headers: dict = {},
    session: Session | None = None,
):
    current_url = rel["href"]
    s = session or requests
    if page_size:
        current_url = re.sub(r"page_size=(?:[0-9]+)", f"page_size={page_size}", current_url)
    if not quiet:
        print(f"Fetching {current_url}...")
    while current_url is not None:
        r = s.get(current_url, headers=headers)
        r.raise_for_status()
        payload = r.json()
        total_pages = math.ceil(payload["total"] / payload["page_size"])
        if not quiet:
            print(f"Handling page {payload['page']}/{total_pages}")
        current_url = payload["next_page"]
        for d in payload["data"]:
            yield d
