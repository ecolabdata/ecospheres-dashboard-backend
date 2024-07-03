from datetime import datetime
from typing import TypedDict, List

from sqlalchemy.dialects.postgresql import JSONB


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
    created_at: datetime
    private: bool
    acronym: str
    slug: str
    spatial: dict
    contact_point: dict
    deleted: bool
    description: str
    tags: list
    frequency: str
    temporal_coverage: dict
    license: str
    quality: dict
    internal: dict


class Dataset:

    @classmethod
    def from_payload(cls, payload: dict) -> DatasetRow:
        return DatasetRow(
            dataset_id=payload["id"],
            title=payload["title"],
            extras=payload["extras"] or {},
            harvest_extras=payload["harvest"] or {},
            last_modified=datetime.fromisoformat(payload["last_modified"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            slug=payload["slug"],
            acronym=payload["acronym"],
            private=payload["private"],
            spatial=payload["spatial"] or {},
            contact_point=payload["contact_point"] or {},
            organization=payload["organization"]["id"] if payload["organization"] else None,
            owner=payload["owner"]["id"] if payload["owner"] else None,
            nb_resources=payload["resources"]["total"],
            description=payload["description"],
            frequency=payload["frequency"],
            tags=payload["tags"] or [],
            temporal_coverage=payload["temporal_coverage"] or {},
            license=payload["license"],
            quality=payload["quality"] or {},
            internal=payload["internal"] or {},
            deleted=False,
        )

    @classmethod
    def col_types(cls):
        return {
            "tags": JSONB,
        }


class ResourceRow(TypedDict):
    dataset_id: str
    resource_id: str
    title: str
    description: str
    type: str
    format: str | None
    url: str
    latest: str
    checksum: dict
    filesize: int | None
    mime: str | None
    created_at: datetime
    last_modified: datetime
    harvest_extras: dict
    internal: dict
    schema: dict


class Resource:

    @classmethod
    def from_payload(cls, dataset_id: str, payload: dict) -> ResourceRow | None:
        return ResourceRow(
            dataset_id=dataset_id,
            resource_id=payload["id"],
            title=payload["title"],
            description=payload["description"],
            type=payload["type"],
            format=payload["format"],
            url=payload["url"],
            latest=payload["latest"],
            checksum=payload["checksum"] or {},
            filesize=payload["filesize"],
            mime=payload["mime"],
            last_modified=datetime.fromisoformat(payload["last_modified"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            harvest_extras=payload["harvest"] or {},
            internal=payload["internal"] or {},
            schema=payload["schema"] or {},
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
            for dataset in payload["extras"]["ecospheres"]["datasets_properties"]
            if dataset.get("id")
        ]
