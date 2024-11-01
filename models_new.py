import re
from datetime import datetime
from typing import List, Optional

from sqlalchemy import JSON, Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column, relationship

MISSING_PREFIX_MESSAGE = "[préfixe absent]"

DEFAULT_EXCLUDE = (None,)
DEFAULT_LIST_EXCLUDE = (None, [])
DEFAULT_STRING_EXCLUDE = (None, "")

Base = declarative_base()


def exists(element, exclude: tuple = DEFAULT_EXCLUDE):
    return element not in exclude


class DatasetComputedColumns:
    MISSING_PREFIX_MESSAGE = "[préfixe absent]"

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

    def __init__(self, payload: dict, prefix: str, licenses: list) -> None:
        self.payload = payload
        self.prefix = prefix
        self.licenses = licenses

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
        id = self.payload["dataset_id"]
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

    def get_harvest_info(self, keys: list[str]) -> dict:
        harvest = self.payload.get("harvest", {})
        return (
            {f"harvest__{key}": val for key, val in harvest.items() if f"harvest__{key}" in keys}
            if harvest
            else {}
        )

    def get_license_title(self) -> str | None:
        license_id = self.payload.get("license")
        return next((item["title"] for item in self.licenses if item["id"] == license_id), None)

    def get_computed_columns(self):
        return {
            "prefix_harvest_remote_id": self.get_prefix_or_fallback_from("remote_id"),
            "prefix_harvest_remote_url": self.get_prefix_or_fallback_from("remote_url"),
            "url_data_gouv": self.get_url_data_gouv(),
            "consistent_dates": self.get_consistent_dates(),
            "consistent_temporal_coverage": self.get_consistent_temporal_coverage(),
            "license__title": self.get_license_title(),
        }


class Dataset(Base):
    __tablename__ = "catalog"

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    organization: Mapped[Optional[str]] = mapped_column(ForeignKey("organizations.organization_id"))
    owner: Mapped[Optional[str]]
    nb_resources: Mapped[int]
    extras: Mapped[dict] = mapped_column(JSON)
    last_modified: Mapped[datetime]
    created_at: Mapped[datetime]
    private: Mapped[bool]
    acronym: Mapped[Optional[str]]
    slug: Mapped[str]
    spatial: Mapped[Optional[dict]] = mapped_column(JSON)
    contact_point: Mapped[Optional[dict]] = mapped_column(JSON)
    deleted: Mapped[bool]
    description: Mapped[str]
    frequency: Mapped[str]
    temporal_coverage: Mapped[Optional[dict]] = mapped_column(JSON)
    license: Mapped[str]
    license__title: Mapped[str]
    quality: Mapped[dict] = mapped_column(JSON)
    internal: Mapped[dict] = mapped_column(JSON)

    # harvest info columns
    harvest__backend: Mapped[Optional[str]]
    harvest__created_at: Mapped[Optional[datetime]]
    harvest__dct_identifier: Mapped[Optional[str]]
    harvest__domain: Mapped[Optional[str]]
    harvest__last_update: Mapped[Optional[datetime]]
    harvest__modified_at: Mapped[Optional[datetime]]
    harvest__remote_id: Mapped[Optional[str]]
    harvest__remote_url: Mapped[Optional[str]]
    harvest__source_id: Mapped[Optional[str]]
    harvest__uri: Mapped[Optional[str]]

    # indicators columns
    has_license: Mapped[bool]
    has_harvest__created_at: Mapped[bool]
    has_harvest__modified_at: Mapped[bool]
    has_harvest__remote_id: Mapped[bool]
    has_harvest__remote_url: Mapped[bool]
    has_resources__total: Mapped[bool]
    has_spatial__zones: Mapped[bool]
    has_spatial__geom: Mapped[bool]
    has_temporal_coverage: Mapped[bool]
    has_frequency: Mapped[bool]
    has_contact_point: Mapped[bool]

    # other computed columns
    prefix_harvest_remote_id: Mapped[str]
    prefix_harvest_remote_url: Mapped[str]
    url_data_gouv: Mapped[str]
    consistent_dates: Mapped[bool]
    consistent_temporal_coverage: Mapped[bool]

    # relationships
    resources: Mapped[List["Resource"]] = relationship("Resource", back_populates="dataset")
    bouquets: Mapped[List["DatasetBouquet"]] = relationship(
        "DatasetBouquet", back_populates="dataset"
    )
    # Add the relationship with a different name, so as not to clash with the existing foreign key
    organization_rel: Mapped[Optional["Organization"]] = relationship(
        "Organization", foreign_keys=[organization], back_populates="datasets"
    )

    def __repr__(self):
        return f"<Dataset {self.dataset_id}>"

    @classmethod
    def from_payload(cls, payload: dict, prefix: str, licenses: list) -> "Dataset":
        """Build a Dataset instance from an API payload"""
        data = payload.copy()
        data["deleted"] = False

        # some attributes need explicit mapping, for safety or casting,
        # the others will be taken as is from payload if they're defined on class
        data["dataset_id"] = data.pop("id")
        data["nb_resources"] = data["resources"]["total"]
        # conflicts with relationship, needs to be removed
        data.pop("resources")
        data["organization"] = data["organization"]["id"] if data["organization"] else None
        data["owner"] = data["owner"]["id"] if data["owner"] else None
        # FIXME: we probably don't need those anymore, the column is explicitly JSON
        # --> write a migration to migrate {} to null and remove
        data["extras"] = data["extras"] or {}
        data["spatial"] = data["spatial"] or {}
        data["contact_point"] = data["contact_point"] or {}
        data["temporal_coverage"] = data["temporal_coverage"] or {}
        data["quality"] = data["quality"] or {}
        data["internal"] = data["internal"] or {}

        computer = DatasetComputedColumns(data, prefix, licenses)

        computed_columns = computer.get_computed_columns()
        indicators = computer.get_indicators()
        harvest_info = computer.get_harvest_info(
            [k for k in cls.__dict__.keys() if k.startswith("harvest__")]
        )

        db_data = {
            **{k: v for k, v in data.items() if hasattr(cls, k)},
            **computed_columns,
            **indicators,
            **harvest_info,
        }

        return cls(**db_data)


class ResourceComputedColumns:
    def __init__(self, payload: dict):
        self.payload = payload

    def get_indicators(self) -> dict:
        return {
            "title__exists": exists(self.payload["title"], exclude=DEFAULT_STRING_EXCLUDE),
            "description__exists": exists(
                self.payload["description"], exclude=DEFAULT_STRING_EXCLUDE
            ),
            "type__exists": exists(self.payload["type"], exclude=DEFAULT_STRING_EXCLUDE),
            "format__exists": exists(self.payload["format"], exclude=DEFAULT_STRING_EXCLUDE),
        }


class Resource(Base):
    __tablename__ = "resources"

    id: Mapped[int] = mapped_column(primary_key=True)
    resource_id: Mapped[str]
    title: Mapped[Optional[str]]
    description: Mapped[Optional[str]]
    type: Mapped[Optional[str]]
    format: Mapped[Optional[str]]
    url: Mapped[str]
    latest: Mapped[str]
    checksum: Mapped[Optional[dict]] = mapped_column(JSON)
    filesize: Mapped[Optional[int]]
    mime: Mapped[Optional[str]]
    created_at: Mapped[datetime]
    last_modified: Mapped[datetime]
    harvest: Mapped[Optional[dict]] = mapped_column(JSON)
    internal: Mapped[dict] = mapped_column(JSON)
    schema: Mapped[Optional[dict]] = mapped_column(JSON)

    # indicators columns
    title__exists: Mapped[bool]
    description__exists: Mapped[Optional[bool]]
    type__exists: Mapped[Optional[bool]]
    format__exists: Mapped[Optional[bool]]

    # relationships
    dataset_id: Mapped[str] = mapped_column(ForeignKey("catalog.dataset_id"))
    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="resources")

    def __repr__(self):
        return f"<Resource {self.resource_id} of {self.dataset!r}>"

    @classmethod
    def from_payload(cls, payload: dict, dataset_id: str) -> "Resource":
        data = payload.copy()
        data["resource_id"] = data.pop("id")

        computer = ResourceComputedColumns(data)

        return cls(
            **{
                **{k: v for k, v in data.items() if hasattr(cls, k)},
                **computer.get_indicators(),
                "dataset_id": dataset_id,
            }
        )


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[str] = mapped_column(unique=True)
    name: Mapped[str]
    acronym: Mapped[Optional[str]]
    service_public: Mapped[bool]

    # relationships
    datasets: Mapped[List["Dataset"]] = relationship(
        "Dataset", foreign_keys="Dataset.organization", back_populates="organization_rel"
    )

    def __repr__(self):
        return f"<Organization {self.organization_id}>"

    @classmethod
    def from_payload(cls, payload: dict) -> "Organization":
        data = payload.copy()
        data["organization_id"] = data.pop("id")

        return cls(
            **{
                **{k: v for k, v in data.items() if hasattr(cls, k)},
                "service_public": all(
                    k in [b["kind"] for b in data.get("badges", [])]
                    for k in ["public-service", "certified"]
                ),
            }
        )


class Bouquet(Base):
    __tablename__ = "bouquets"

    id = Column(Integer, primary_key=True)
    bouquet_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    private = Column(Boolean)
    organization = Column(String)
    owner = Column(String)
    extras = Column(JSON)
    last_modified = Column(DateTime)
    created_at = Column(DateTime)
    nb_datasets = Column(Integer)
    nb_factors = Column(Integer)
    deleted = Column(Boolean)

    datasets = relationship("DatasetBouquet", back_populates="bouquet")

    def __repr__(self):
        return f"<Bouquet {self.bouquet_id}>"

    @classmethod
    def from_payload(cls, data: dict) -> "Bouquet":
        datasets_properties = data["extras"]["ecospheres"]["datasets_properties"]
        return cls(
            **{
                "bouquet_id": data["id"],
                "name": data["name"],
                "private": data["private"],
                "last_modified": datetime.fromisoformat(data["last_modified"]),
                "created_at": datetime.fromisoformat(data["created_at"]),
                "organization": data["organization"]["id"] if data["organization"] else None,
                "owner": data["owner"]["id"] if data["owner"] else None,
                "extras": data["extras"],
                "nb_datasets": len([d for d in datasets_properties if d.get("id")]),
                "nb_factors": len(datasets_properties),
                "deleted": False,
            }
        )


# FIXME: does not match real schema (duplicated dataset columns)
# the fix is probably to remove the existing table
class DatasetBouquet(Base):
    __tablename__ = "datasets_bouquets"

    id = Column(Integer, primary_key=True)
    bouquet_id = Column(String, ForeignKey("bouquets.bouquet_id"))
    dataset_id = Column(String, ForeignKey("catalog.dataset_id"))

    dataset = relationship("Dataset", back_populates="bouquets")
    bouquet = relationship("Bouquet", back_populates="datasets")

    def __repr__(self):
        return f"<DatasetBouquet of {self.dataset!r} and {self.bouquet!r}>"
