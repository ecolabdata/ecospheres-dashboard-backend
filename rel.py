import math
import re
import time
from typing import TypedDict

import requests


class Rel(TypedDict):
    href: str


def iter_rel(rel: Rel, quiet: bool = False, page_size: int | None = None, headers: dict = {}):
    current_url = rel["href"]
    if page_size:
        current_url = re.sub(r"page_size=(?:[0-9]+)", f"page_size={page_size}", current_url)
    if not quiet:
        print(f"Fetching {current_url}...")
    while current_url is not None:
        while True:
            r = requests.get(current_url, headers=headers)
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
