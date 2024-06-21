from datetime import datetime
from typing import TypedDict, List


class Rel(TypedDict):
    href: str


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
            deleted=False,
        )


class OrganizationRow(TypedDict):
    organization_id: str
    name: str
    acronym: str
    service_public: bool


class Organization:

    @classmethod
    def from_payload(cls, payload: dict) -> OrganizationRow:
        return OrganizationRow(
            organization_id=payload["id"],
            name=payload["name"],
            acronym=payload["acronym"],
            service_public=all(k in payload["badges"] for k in ["public-service", "certified"]),
        )


class BouquetRow(TypedDict):
    dataset_id: str
    bouquet_id: str
    name: str


class Bouquet:

    @classmethod
    def from_payload(cls, payload: dict) -> List[BouquetRow]:
        return [
            BouquetRow(
                dataset_id=dataset["id"],
                bouquet_id=payload["id"],
                name=payload["name"],
            )
            for dataset in payload["extras"]["ecospheres:datasets_properties"]
            if dataset.get("id")
        ]
