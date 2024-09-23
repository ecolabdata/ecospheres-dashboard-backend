from datetime import datetime
from typing import TypedDict, List, OrderedDict
import re

from dataset import Table
from sqlalchemy.dialects.postgresql import JSONB


class BaseModel:

    indicators = []

    def __init__(self, payload: dict) -> None:
        self.payload = payload

    def get_attr_by_path(self, path: str, sep: str = "__"):
        parts = path.split(sep)
        current_level = self.payload

        for part in parts:
            try:
                current_level = current_level[part]
                if current_level is None:
                    return None
            except KeyError:
                return None

        return current_level

    def get_indicators(self) -> dict:
        indicators = {}
        for indicator in self.indicators:
            value = self.get_attr_by_path(indicator["id"])
            cmp = indicator["not"] if isinstance(indicator["not"], list) else [indicator["not"]]
            indicators[f"has_{indicator['id']}"] = all(value != c for c in cmp)

        return indicators

    def get_prefix_or_fallback_from(self, url: str, message: str) -> str:
        m = re.match("^(.*/)[^/]+$", url)

        if m:
            return m.group(1)

        return message

    def compute_prefix_harvest_remote_id(self) -> str:
        missing_prefix_message = 'Préfixe manquant'

        try:
            remote_id = self.payload['harvest']['remote_id']
        except KeyError:
            return missing_prefix_message

        return self.get_prefix_or_fallback_from(
            remote_id,
            missing_prefix_message
        )

    def compute_prefix_harvest_remote_url(self) -> str:
        missing_prefix_message = 'Préfixe manquant'

        try:
            remote_url = self.payload['harvest']['remote_url']
        except KeyError:
            return missing_prefix_message

        return self.get_prefix_or_fallback_from(
            remote_url,
            missing_prefix_message
        )

    def get_url_data_gouv(self):
        url = 'https://demo.data.gouv.fr/fr/datasets/'
        id = self.payload['id']

        return "<a href=\"{url}{id}\" target=\"_blank\">{url}{id}</a>".format(
            url=url, id=id
        )

    def get_consistent_date(self) -> str:
        try:
            created_at = self.payload['created_at']
        except KeyError:
            return None

        try:
            modified_at = self.payload['last_modified']
        except KeyError:
            return created_at

        if (modified_at and modified_at >= created_at):
            return modified_at

        return created_at

    def get_consistent_temporal_coverage(self) -> bool:
        temporal_coverage = self.payload['temporal_coverage']

        if temporal_coverage is None:
            return False

        try:
            start = temporal_coverage['start']
        except KeyError:
            return False

        try:
            end = temporal_coverage['end']
        except KeyError:
            return False

        return end >= start

    def get_harvest_remote_columns(self) -> dict:
        return {
            'prefix_harvest_remote_id': self.compute_prefix_harvest_remote_id(),
            'prefix_harvest_remote_url': self.compute_prefix_harvest_remote_url(),
            'url_data_gouv': self.get_url_data_gouv(),
            'consistent_date': self.get_consistent_date(),
            'consistent_temporal_coverage': self.get_consistent_temporal_coverage()
        }

    def to_model(self):
        raise NotImplementedError()

    def to_row(self) -> dict:
        model = self.to_model()
        indicators = self.get_indicators()
        harvest_remote_columns = self.get_harvest_remote_columns()
        return {**model, **indicators, **harvest_remote_columns}


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
    # FIXME: make metabase schema sync crash
    # ERROR: duplicate key value violates unique constraint "idx_uniq_field_table_id_parent_id_name_2col",  Detail: Key (table_id, name)=(9, tags) already exists.  # noqa
    # tags: list
    frequency: str
    temporal_coverage: dict
    license: str
    quality: dict
    internal: dict


class Dataset(BaseModel):

    indicators = [
        {"id": "license", "not": [None, "notspecified"]},
        {"id": "harvest__created_at", "not": None},
        {"id": "harvest__modified_at", "not": None},
        {"id": "harvest__remote_id", "not": None},
        {"id": "harvest__remote_url", "not": None},
        {"id": "resources__total", "not": 0},
        {"id": "spatial__zones", "not": [[], None]},
        {"id": "spatial__geom", "not": [[], None]},
        {"id": "temporal_coverage", "not": None},
        {"id": "frequency", "not": [None, "unknown"]},
        {"id": "contact_point", "not": None},
    ]

    def to_model(self) -> DatasetRow:
        return DatasetRow(
            dataset_id=self.payload["id"],
            title=self.payload["title"],
            extras=self.payload["extras"] or {},
            harvest_extras=self.payload["harvest"] or {},
            last_modified=datetime.fromisoformat(self.payload["last_modified"]),
            created_at=datetime.fromisoformat(self.payload["created_at"]),
            slug=self.payload["slug"],
            acronym=self.payload["acronym"],
            private=self.payload["private"],
            spatial=self.payload["spatial"] or {},
            contact_point=self.payload["contact_point"] or {},
            organization=self.payload["organization"]["id"] if self.payload["organization"] else None,
            owner=self.payload["owner"]["id"] if self.payload["owner"] else None,
            nb_resources=self.payload["resources"]["total"],
            description=self.payload["description"],
            frequency=self.payload["frequency"],
            # tags=self.payload["tags"] or [],
            temporal_coverage=self.payload["temporal_coverage"] or {},
            license=self.payload["license"],
            quality=self.payload["quality"] or {},
            internal=self.payload["internal"] or {},
            deleted=False,
        )

    @classmethod
    def from_record(cls, record: OrderedDict) -> DatasetRow:
        return DatasetRow(**{k: v for k, v in record.items() if k != "id"})

    @classmethod
    def col_types(cls):
        return {
            # "tags": JSONB,
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


class DatasetBouquetRow(DatasetRow):
    bouquet_id: str
    bouquet_name: str


class DatasetBouquet:

    @classmethod
    def from_payload(cls, payload: dict, catalog: Table) -> List[DatasetBouquetRow]:
        """
        Serialize a list of datasets existing in catalog from a bouquet payload
        """
        datasets_ids = [
            dataset["id"] for dataset in payload["extras"]["ecospheres"]["datasets_properties"]
            if dataset.get("id")
        ]
        datasets = [
            result for result in (catalog.find_one(dataset_id=did) for did in datasets_ids)
            if result is not None
        ]

        return [
            {
                "bouquet_id": payload["id"],
                "bouquet_name": payload["name"],
                **Dataset.from_record(dataset)
            }
            for dataset in datasets
        ]


class BouquetRow(TypedDict):
    bouquet_id: str
    name: str
    private: bool
    organization: str | None
    owner: str | None
    extras: dict
    last_modified: datetime
    created_at: datetime
    nb_datasets: int
    nb_factors: int
    deleted: bool


class Bouquet:

    @classmethod
    def from_payload(cls, payload: dict) -> BouquetRow:
        datasets_properties = payload["extras"]["ecospheres"]["datasets_properties"]
        return BouquetRow(
            bouquet_id=payload["id"],
            name=payload["name"],
            private=payload["private"],
            last_modified=datetime.fromisoformat(payload["last_modified"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            organization=payload["organization"]["id"] if payload["organization"] else None,
            owner=payload["owner"]["id"] if payload["owner"] else None,
            extras=payload["extras"] or {},
            nb_datasets=len([d for d in datasets_properties if d.get("id")]),
            nb_factors= len(datasets_properties),
            deleted=False,
        )
