from datetime import datetime
from typing import TypedDict


class DatasetRow(TypedDict):
    dataset_id: str
    title: str
    organization: str | None
    owner: str | None
    nb_resources: int
    extras: dict
    harvest_extras: dict
    last_modified: datetime
    spatial: dict
    contact_point: dict
    deleted: bool


class OrganizationRow(TypedDict):
    organization_id: str
    name: str
    acronym: str
    service_public: bool


class Dataset:

    @classmethod
    def from_payload(cls, payload: dict) -> DatasetRow:
        return DatasetRow(
            dataset_id=payload["id"],
            title=payload["title"],
            extras=payload["extras"] or {},
            harvest_extras=payload["harvest"] or {},
            last_modified=datetime.fromisoformat(payload["last_modified"]),
            spatial=payload["spatial"] or {},
            contact_point=payload["contact_point"] or {},
            organization=payload["organization"]["id"] if payload["organization"] else None,
            owner=payload["owner"]["id"] if payload["owner"] else None,
            nb_resources=payload["resources"]["total"],
            deleted=False
        )


class Organization:

    @classmethod
    def from_payload(cls, payload: dict) -> OrganizationRow:
        return OrganizationRow(
            organization_id=payload["id"],
            name=payload["name"],
            acronym=payload["acronym"],
            service_public=all(k in payload["badges"] for k in ["public-service", "certified"])
        )
