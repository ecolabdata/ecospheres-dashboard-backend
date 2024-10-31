import re
from collections import defaultdict
from datetime import datetime
from typing import Callable, Type, TypedDict

from sqlalchemy import JSON, Boolean, Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

MISSING_PREFIX_MESSAGE = "[préfixe absent]"

DEFAULT_EXCLUDE = (None,)
DEFAULT_LIST_EXCLUDE = (None, [])
DEFAULT_STRING_EXCLUDE = (None, "")

Base = declarative_base()


def exists(element, exclude: tuple = DEFAULT_EXCLUDE):
    return element not in exclude


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


class DatasetComputedColumns:
    MISSING_PREFIX_MESSAGE = "[préfixe absent]"
    TYPE_MAP: dict[Type, Callable] = defaultdict(
        # Callable (value) must be a constructor for corresponding Type (key)
        # This can't be enforced with type hints, so use tests!
        lambda: lambda v: v,  # default constructor is the type's own constructor
        {datetime: datetime.fromisoformat},
    )

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

    def get_harvest_info(self) -> dict:
        harvest = self.payload.get("harvest", {})
        if isinstance(harvest, dict):
            for key, value in harvest.items():
                if isinstance(value, str) and value.endswith("Z"):
                    try:
                        harvest[key] = datetime.fromisoformat(value.rstrip("Z"))
                    except ValueError:
                        pass

        info = HarvestInfo()
        if harvest:
            # No type hints at runtime, we rely on TYPE_MAP being correct
            for k, v in harvest.items():
                t = HarvestInfo.__annotations__.get(str(k))
                if t:
                    info[k] = self.TYPE_MAP[t](v)

        return {f"harvest__{key}": val for key, val in harvest.items()} if harvest else {}

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

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String, unique=True, nullable=False)
    title = Column(String, nullable=False)
    organization = Column(String, ForeignKey("organizations.organization_id"))
    owner = Column(String)
    nb_resources = Column(Integer)
    extras = Column(JSON)
    last_modified = Column(DateTime)
    created_at = Column(DateTime)
    private = Column(Boolean)
    acronym = Column(String)
    slug = Column(String)
    spatial = Column(JSON)
    contact_point = Column(JSON)
    deleted = Column(Boolean)
    description = Column(String)
    frequency = Column(String)
    temporal_coverage = Column(JSON)
    license = Column(String)
    license__title = Column(String)
    quality = Column(JSON)
    internal = Column(JSON)
    # harvest info columns, extracted from harvest
    harvest__backend = Column(String)
    harvest__created_at = Column(DateTime)
    harvest__dct_identifier = Column(String)
    harvest__domain = Column(String)
    # FIXME: those two should be handled by sqlalchemy without type casting
    harvest__last_update = Column(DateTime)
    harvest__modified_at = Column(DateTime)
    harvest__remote_id = Column(String)
    harvest__remote_url = Column(String)
    harvest__source_id = Column(String)
    harvest__uri = Column(String)
    # indicators columns, computed
    has_license = Column(Boolean)
    has_harvest__created_at = Column(Boolean)
    has_harvest__modified_at = Column(Boolean)
    has_harvest__remote_id = Column(Boolean)
    has_harvest__remote_url = Column(Boolean)
    has_resources__total = Column(Boolean)
    has_spatial__zones = Column(Boolean)
    has_spatial__geom = Column(Boolean)
    has_temporal_coverage = Column(Boolean)
    has_frequency = Column(Boolean)
    has_contact_point = Column(Boolean)
    # other computed columns
    prefix_harvest_remote_id = Column(String)
    prefix_harvest_remote_url = Column(String)
    url_data_gouv = Column(String)
    consistent_dates = Column(Boolean)
    consistent_temporal_coverage = Column(Boolean)

    resources = relationship("Resource", back_populates="dataset")
    bouquets = relationship("DatasetBouquet", back_populates="dataset")

    @classmethod
    def from_payload(cls, data: dict, prefix: str, licenses: list) -> "Dataset":
        """Build a Dataset instance from an API payload"""
        data["deleted"] = False

        # some attributes need explicit mapping, for safety or casting,
        # the others will be taken as is from payload if they're defined on class
        # FIXME: we should remove the id column and use dataset_id as primary key
        data["dataset_id"] = data.pop("id")
        data["nb_resources"] = data["resources"]["total"]
        data.pop("resources")
        data["organization"] = data["organization"]["id"] if data["organization"] else None
        data["owner"] = data["owner"]["id"] if data["owner"] else None
        # FIXME: we probably don't need those anymore, the column is explicitly JSON
        # write a migration to migrate {} to null and remove
        data["extras"] = data["extras"] or {}
        data["spatial"] = data["spatial"] or {}
        data["contact_point"] = data["contact_point"] or {}
        data["temporal_coverage"] = data["temporal_coverage"] or {}
        data["quality"] = data["quality"] or {}
        data["internal"] = data["internal"] or {}

        computer = DatasetComputedColumns(data, prefix, licenses)

        computed_columns = computer.get_computed_columns()
        indicators = computer.get_indicators()
        harvest_info = computer.get_harvest_info()

        db_data = {
            **{k: v for k, v in data.items() if hasattr(cls, k)},
            **computed_columns,
            **indicators,
            **harvest_info,
        }

        print(db_data)

        return cls(**db_data)


class Resource(Base):
    __tablename__ = "resources"

    id = Column(Integer, primary_key=True)
    dataset_id = Column(String, ForeignKey("catalog.dataset_id"))
    dataset = relationship("Dataset", back_populates="resources")
    resource_id = Column(String, nullable=False)
    title = Column(String)
    title__exists = Column(Boolean)
    description = Column(String)
    description__exists = Column(Boolean)
    type = Column(String)
    type__exists = Column(Boolean)
    format = Column(String)
    format__exists = Column(Boolean)
    url = Column(String)
    latest = Column(String)
    checksum = Column(JSON)
    filesize = Column(Integer)
    mime = Column(String)
    created_at = Column(DateTime)
    last_modified = Column(DateTime)
    harvest = Column(JSON)
    internal = Column(JSON)
    schema = Column(JSON)

    @classmethod
    def from_payload(cls, data: dict, dataset_id: str) -> "Resource":
        return cls(
            **{
                **{k: v for k, v in data.items() if hasattr(cls, k)},
                dataset_id: dataset_id,
            }
        )


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True)
    organization_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    acronym = Column(String)
    service_public = Column(Boolean)

    @classmethod
    def from_payload(cls, data: dict) -> "Organization":
        return cls(
            **{
                **{k: v for k, v in data.items() if hasattr(cls, k)},
                "service_public": all(
                    k in data.get("badges", []) for k in ["public-service", "certified"]
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
