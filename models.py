import re
from collections import defaultdict
from datetime import datetime
from typing import Callable, List, OrderedDict, Type, TypedDict

from dataset import Table

DEFAULT_EXCLUDE = (None,)
DEFAULT_LIST_EXCLUDE = (None, [])
DEFAULT_STRING_EXCLUDE = (None, "")


def exists(element, exclude: tuple = DEFAULT_EXCLUDE):
    return element not in exclude


class BaseModel:
    MISSING_PREFIX_MESSAGE = "[préfixe absent]"

    indicators = []

    def __init__(self, payload: dict, prefix: str) -> None:
        self.payload = payload
        self.prefix = prefix

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
            field = indicator["field"]
            value = self.get_attr_by_path(field)
            indicators[f"has_{field}"] = exists(value, exclude=indicator["exclude"])
        return indicators

    def get_prefix_or_fallback_from(self, key) -> str:
        try:
            harvest = self.payload["harvest"]

            if harvest is None:
                raise KeyError

            url = harvest[key]

            if url is None:
                raise KeyError

        except KeyError:
            return self.MISSING_PREFIX_MESSAGE

        m = re.match("^(.*/)[^/]+$", url)

        if m:
            return m.group(1)

        return self.MISSING_PREFIX_MESSAGE

    def get_url_data_gouv(self) -> str:
        url = f"https://{self.prefix}.data.gouv.fr/fr/datasets/"
        id = self.payload["id"]

        return f'<a href="{url}{id}" target="_blank">{id}</a>'

    def get_consistent_dates(self) -> bool:
        created_at = self.payload.get("created_at")
        modified_at = self.payload.get("last_modified")

        if created_at is None:
            return modified_at is None

        if modified_at is None:
            return True

        return modified_at >= created_at

    def get_consistent_temporal_coverage(self) -> bool:
        temporal_coverage = self.payload["temporal_coverage"]

        if temporal_coverage is None:
            return True

        start = temporal_coverage.get("start")
        end = temporal_coverage.get("end")

        if start is None:
            return end is None

        if end is None:
            return False

        return end > start

    def to_model(self):
        raise NotImplementedError()

    def to_row(self) -> dict:
        model = self.to_model()
        indicators = self.get_indicators()
        computed_columns = {
            "prefix_harvest_remote_id": self.get_prefix_or_fallback_from("remote_id"),
            "prefix_harvest_remote_url": self.get_prefix_or_fallback_from("remote_url"),
            "url_data_gouv": self.get_url_data_gouv(),
            "consistent_dates": self.get_consistent_dates(),
            "consistent_temporal_coverage": self.get_consistent_temporal_coverage(),
        }

        harvest = {f"harvest__{key}": val for key, val in model["harvest"].items()}
        del model["harvest"]

        return {**model, **indicators, **computed_columns, **harvest}


class Rel(TypedDict):
    href: str


class HarvestInfo(TypedDict, total=False):
    # All harvest elements are optional in the udata model => total=False
    backend: str
    created_at: datetime
    dct_identifier: str
    domain: str
    last_update: datetime
    modified_at: datetime
    remote_id: str
    remote_url: str
    source_id: str
    uri: str


class DatasetRow(TypedDict):
    dataset_id: str
    title: str
    organization: str | None
    owner: str | None
    nb_resources: int
    extras: dict
    harvest: HarvestInfo
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
    license__title: str | None
    quality: dict
    internal: dict


class Dataset(BaseModel):
    TYPE_MAP: dict[Type, Callable] = defaultdict(
        # Callable (value) must be a constructor for corresponding Type (key)
        # This can't be enforced with type hints, so use tests!
        lambda: lambda v: v,  # default constructor is the type's own constructor
        {datetime: datetime.fromisoformat},
    )

    def __init__(self, payload: dict, prefix: str, licenses: list = []) -> None:
        super().__init__(payload, prefix)
        self.licenses = licenses

    indicators = [
        {"field": "license", "exclude": DEFAULT_STRING_EXCLUDE + ("notspecified",)},
        {"field": "harvest__created_at", "exclude": DEFAULT_EXCLUDE},
        {"field": "harvest__modified_at", "exclude": DEFAULT_EXCLUDE},
        {"field": "harvest__remote_id", "exclude": DEFAULT_STRING_EXCLUDE},
        {"field": "harvest__remote_url", "exclude": DEFAULT_STRING_EXCLUDE},
        {"field": "resources__total", "exclude": (0,)},
        {"field": "spatial__zones", "exclude": DEFAULT_LIST_EXCLUDE},
        {"field": "spatial__geom", "exclude": DEFAULT_LIST_EXCLUDE},
        {"field": "temporal_coverage", "exclude": DEFAULT_EXCLUDE},
        {"field": "frequency", "exclude": DEFAULT_STRING_EXCLUDE + ("unknown",)},
        {"field": "contact_point", "exclude": DEFAULT_EXCLUDE},
    ]

    def get_harvest_info(self, harvest: dict | None) -> HarvestInfo:
        info = HarvestInfo()
        if harvest:
            # No type hints at runtime, we rely on TYPE_MAP being correct
            for k, v in harvest.items():
                t = HarvestInfo.__annotations__.get(str(k))
                if t:
                    info[k] = Dataset.TYPE_MAP[t](v)

        return info

    def get_license_title(self, id: str | None) -> str | None:
        return next((item["title"] for item in self.licenses if item["id"] == id), None)

    def to_model(self) -> DatasetRow:
        return DatasetRow(
            dataset_id=self.payload["id"],
            title=self.payload["title"],
            extras=self.payload["extras"] or {},
            harvest=self.get_harvest_info(self.payload["harvest"]),
            last_modified=datetime.fromisoformat(self.payload["last_modified"]),
            created_at=datetime.fromisoformat(self.payload["created_at"]),
            slug=self.payload["slug"],
            acronym=self.payload["acronym"],
            private=self.payload["private"],
            spatial=self.payload["spatial"] or {},
            contact_point=self.payload["contact_point"] or {},
            organization=self.payload["organization"]["id"]
            if self.payload["organization"]
            else None,
            owner=self.payload["owner"]["id"] if self.payload["owner"] else None,
            nb_resources=self.payload["resources"]["total"],
            description=self.payload["description"],
            frequency=self.payload["frequency"],
            # tags=self.payload["tags"] or [],
            temporal_coverage=self.payload["temporal_coverage"] or {},
            license=self.payload["license"],
            license__title=self.get_license_title(self.payload["license"]),
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
    title__exists: bool
    description: str
    description__exists: bool
    type: str
    type__exists: bool
    format: str | None
    format__exists: bool
    url: str
    latest: str
    checksum: dict
    filesize: int | None
    mime: str | None
    created_at: datetime
    last_modified: datetime
    harvest: dict
    internal: dict
    schema: dict


class Resource:
    @classmethod
    def from_payload(cls, dataset_id: str, payload: dict) -> ResourceRow | None:
        return ResourceRow(
            dataset_id=dataset_id,
            resource_id=payload["id"],
            title=payload["title"],
            title__exists=exists(payload["title"], exclude=DEFAULT_STRING_EXCLUDE),
            description=payload["description"],
            description__exists=exists(payload["description"], exclude=DEFAULT_STRING_EXCLUDE),
            type=payload["type"],
            type__exists=exists(payload["type"], exclude=DEFAULT_STRING_EXCLUDE),
            format=payload["format"],
            format__exists=exists(payload["format"], exclude=DEFAULT_STRING_EXCLUDE),
            url=payload["url"],
            latest=payload["latest"],
            checksum=payload["checksum"] or {},
            filesize=payload["filesize"],
            mime=payload["mime"],
            last_modified=datetime.fromisoformat(payload["last_modified"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            harvest=payload["harvest"] or {},
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
            dataset["id"]
            for dataset in payload["extras"]["ecospheres"]["datasets_properties"]
            if dataset.get("id")
        ]
        datasets = [
            result
            for result in (catalog.find_one(dataset_id=did) for did in datasets_ids)
            if result is not None
        ]

        return [
            {
                "bouquet_id": payload["id"],
                "bouquet_name": payload["name"],
                **Dataset.from_record(dataset),
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
            nb_factors=len(datasets_properties),
            deleted=False,
        )
