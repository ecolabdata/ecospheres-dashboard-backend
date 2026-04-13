import math
import re
import time
from logging import Logger
from typing import TypedDict

import requests
from requests.sessions import Session


class Rel(TypedDict):
    href: str


def iter_rel(
    rel: Rel,
    page_size: int | None = None,
    headers: dict = {},
    session: Session | None = None,
    log: Logger | None = None,
):
    current_url = rel["href"]
    s = session or requests
    if page_size:
        current_url = re.sub(r"page_size=(?:[0-9]+)", f"page_size={page_size}", current_url)
    if log:
        log.info(f"Fetching {current_url}...")
    while current_url is not None:
        while True:
            r = s.get(current_url, headers=headers)
            if not r.ok:
                if r.status_code == 429:
                    if log:
                        log.warning("429 hit, waiting a bit")
                    time.sleep(10)
                    continue
                else:
                    r.raise_for_status()
            break
        payload = r.json()
        total_pages = math.ceil(payload["total"] / payload["page_size"])
        if log:
            log.info(f"Handling page {payload['page']}/{total_pages}")
        current_url = payload["next_page"]
        for d in payload["data"]:
            yield d
