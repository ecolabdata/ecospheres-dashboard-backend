import re
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime
from textwrap import shorten
from typing import List, NamedTuple, Optional

from sqlalchemy import ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from utils import (
    DEFAULT_EXCLUDE,
    DEFAULT_JSON_EXCLUDE,
    DEFAULT_LIST_EXCLUDE,
    DEFAULT_STRING_EXCLUDE,
    accept,
)


class Bounds[T: (int, float)](NamedTuple):
    values: list[T]
    open: bool


class Bin(NamedTuple):
    bin: int
    label: str


class ContactPoint(NamedTuple):
    name: str | None
    email: str | None


class Base(DeclarativeBase):
    pass


class DatasetComputedColumns:
    MISSING_PREFIX_MESSAGE = "[préfixe absent]"
    DESCRIPTION_MIN_LENGTH = 200
    DESCRIPTION_UPPER_BOUNDS = Bounds[int]([DESCRIPTION_MIN_LENGTH, 1000, 5000], True)
    QUALITY_SCORE_UPPER_BOUNDS = Bounds[float]([0.2, 0.4, 0.6, 0.8, 1.0], False)
    SPATIAL_COORDINATES_MAX_LENGTH = 500

    indicators = [
        {"field": "license", "exclude": DEFAULT_STRING_EXCLUDE + ("notspecified",)},
        {"field": "harvest", "exclude": DEFAULT_JSON_EXCLUDE},
        {"field": "harvest__created_at", "exclude": DEFAULT_EXCLUDE},
        {"field": "harvest__modified_at", "exclude": DEFAULT_EXCLUDE},
        {"field": "harvest__remote_id", "exclude": DEFAULT_STRING_EXCLUDE},
        {"field": "harvest__remote_url", "exclude": DEFAULT_STRING_EXCLUDE},
        {"field": "resources__total", "exclude": (0,)},
        {"field": "spatial__zones", "exclude": DEFAULT_LIST_EXCLUDE},
        {"field": "spatial__geom", "exclude": DEFAULT_LIST_EXCLUDE},
        {"field": "temporal_coverage", "exclude": DEFAULT_JSON_EXCLUDE},
        {"field": "frequency", "exclude": DEFAULT_STRING_EXCLUDE + ("unknown",)},
        {"field": "contact_points", "exclude": DEFAULT_LIST_EXCLUDE},
    ]

    @staticmethod
    def get_bin[T: (int, float)](value: T, bounds: Bounds[T]) -> Bin:
        bin = bisect_right(bounds.values, value)
        if bin < len(bounds.values):
            label = f"moins de {bounds.values[bin]}"
        else:
            v = bounds.values[-1]
            label = f"au moins {v}" if bounds.open else f"{v}"
        return Bin(bin, label)

    def __init__(self, payload: dict, prefix: str, licenses: list = []) -> None:
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
            indicators[f"has_{field}"] = accept(value, exclude=indicator["exclude"])
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

    def get_year_from_harvest_date(self, field: str) -> int | None:
        if d := (self.payload.get("harvest") or {}).get(field):
            return datetime.fromisoformat(d).year

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

    def get_temporal_coverage_range(self) -> str | None:
        coverage = self.payload["temporal_coverage"]
        if coverage:
            start = coverage.get("start") or "?"
            end = coverage.get("end") or "?"
            return f"{start} - {end}"

    def get_spatial_coordinates(self) -> str | None:
        if coords := ((self.payload.get("spatial") or {}).get("geom") or {}).get("coordinates"):
            return shorten(
                repr(coords), width=self.SPATIAL_COORDINATES_MAX_LENGTH, placeholder="..."
            )

    def get_harvest_info(self, keys: list[str]) -> dict:
        harvest = self.payload.get("harvest") or {}
        return {f"harvest__{key}": val for key, val in harvest.items() if f"harvest__{key}" in keys}

    def get_license_title(self) -> str | None:
        license_id = self.payload.get("license")
        return next((item["title"] for item in self.licenses if item["id"] == license_id), None)

    def get_quality_score(self) -> float:
        quality = self.payload.get("quality") or {}
        return float(quality.get("score", 0))

    def get_first_contact_point(self) -> ContactPoint | None:
        if contacts := self.payload.get("contact_points"):
            return ContactPoint(name=contacts[0].get("name"), email=contacts[0].get("email"))

    def get_computed_columns(self):
        description_length = len(self.payload["description"] or "")
        description_bin = self.get_bin(description_length, self.DESCRIPTION_UPPER_BOUNDS)
        quality_score = self.get_quality_score()
        quality_score_bin = self.get_bin(quality_score, self.QUALITY_SCORE_UPPER_BOUNDS)
        first_contact_point = self.get_first_contact_point()
        return {
            "prefix_harvest_remote_id": self.get_prefix_or_fallback_from("remote_id"),
            "prefix_harvest_remote_url": self.get_prefix_or_fallback_from("remote_url"),
            "url_data_gouv": self.get_url_data_gouv(),
            "harvest__created_at__year": self.get_year_from_harvest_date("created_at"),
            "harvest__modified_at__year": self.get_year_from_harvest_date("modified_at"),
            "consistent_dates": self.get_consistent_dates(),
            "consistent_temporal_coverage": self.get_consistent_temporal_coverage(),
            "temporal_coverage__range": self.get_temporal_coverage_range(),
            "spatial__coordinates": self.get_spatial_coordinates(),
            "license__title": self.get_license_title(),
            "description__length__ok": description_length >= self.DESCRIPTION_MIN_LENGTH,
            "description__length__bin": description_bin.bin,
            "description__length__bin_label": description_bin.label,
            "quality__score": quality_score,
            "quality__score__bin": quality_score_bin.bin,
            "quality__score__bin_label": quality_score_bin.label,
            "contact_points__first__name": first_contact_point.name
            if first_contact_point
            else None,
            "contact_points__first__email": first_contact_point.email
            if first_contact_point
            else None,
        }


class Dataset(Base):
    __tablename__ = "catalog"

    id: Mapped[int] = mapped_column(primary_key=True)
    dataset_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    title: Mapped[str] = mapped_column(String, nullable=False)
    organization: Mapped[Optional[str]] = mapped_column(ForeignKey("organizations.organization_id"))
    owner: Mapped[Optional[str]]
    nb_resources: Mapped[int]
    extras: Mapped[dict] = mapped_column(JSONB)
    last_modified: Mapped[datetime]
    created_at: Mapped[datetime]
    private: Mapped[bool]
    acronym: Mapped[Optional[str]]
    slug: Mapped[str]
    spatial: Mapped[Optional[dict]] = mapped_column(JSONB)
    contact_points: Mapped[List[dict]] = mapped_column(JSONB)
    deleted: Mapped[bool]
    description: Mapped[str]
    frequency: Mapped[str]
    temporal_coverage: Mapped[Optional[dict]] = mapped_column(JSONB)
    license: Mapped[str]
    license__title: Mapped[Optional[str]]
    quality: Mapped[dict] = mapped_column(JSONB)
    internal: Mapped[dict] = mapped_column(JSONB)

    # harvest info columns
    harvest__backend: Mapped[Optional[str]]
    harvest__created_at: Mapped[Optional[datetime]]
    harvest__created_at__year: Mapped[int | None]
    harvest__dct_identifier: Mapped[Optional[str]]
    harvest__domain: Mapped[Optional[str]]
    harvest__last_update: Mapped[Optional[datetime]]
    harvest__modified_at: Mapped[Optional[datetime]]
    harvest__modified_at__year: Mapped[int | None]
    harvest__remote_id: Mapped[Optional[str]]
    harvest__remote_url: Mapped[Optional[str]]
    harvest__source_id: Mapped[Optional[str]]
    harvest__uri: Mapped[Optional[str]]

    # indicators columns
    has_license: Mapped[bool]
    has_harvest: Mapped[bool]
    has_harvest__created_at: Mapped[bool]
    has_harvest__modified_at: Mapped[bool]
    has_harvest__remote_id: Mapped[bool]
    has_harvest__remote_url: Mapped[bool]
    has_resources__total: Mapped[bool]
    has_spatial__zones: Mapped[bool]
    has_spatial__geom: Mapped[bool]
    has_temporal_coverage: Mapped[bool]
    has_frequency: Mapped[bool]
    has_contact_points: Mapped[bool]

    # other computed columns
    prefix_harvest_remote_id: Mapped[str]
    prefix_harvest_remote_url: Mapped[str]
    url_data_gouv: Mapped[str]
    consistent_dates: Mapped[bool]
    consistent_temporal_coverage: Mapped[bool]
    temporal_coverage__range: Mapped[str | None]
    spatial__coordinates: Mapped[str | None]
    description__length__ok: Mapped[bool]
    description__length__bin: Mapped[int]
    description__length__bin_label: Mapped[str]
    quality__score: Mapped[float]
    quality__score__bin: Mapped[int]
    quality__score__bin_label: Mapped[str]
    contact_points__first__name: Mapped[str | None]
    contact_points__first__email: Mapped[str | None]

    # relationships
    resources: Mapped[List["Resource"]] = relationship("Resource", back_populates="dataset")
    bouquets: Mapped[list["Bouquet"]] = relationship(
        "Bouquet", secondary="datasets_bouquets", back_populates="datasets"
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
        data["organization"] = data["organization"]["id"] if data["organization"] else None
        data["owner"] = data["owner"]["id"] if data["owner"] else None

        computer = DatasetComputedColumns(data, prefix, licenses)

        computed_columns = computer.get_computed_columns()
        indicators = computer.get_indicators()
        harvest_info = computer.get_harvest_info(
            [k for k in cls.__dict__.keys() if k.startswith("harvest__")]
        )

        # conflicts with relationship, needs to be removed after indicators are computed
        data.pop("resources")

        return cls(
            **{
                **{k: v for k, v in data.items() if hasattr(cls, k)},
                **computed_columns,
                **indicators,
                **harvest_info,
            }
        )


class ResourceComputedColumns:
    def __init__(self, payload: dict):
        self.payload = payload

    def get_computed_columns(self) -> dict:
        return {
            "schema__name": (self.payload.get("schema") or {}).get("name"),
        }

    def get_indicators(self) -> dict:
        return {
            "title__exists": accept(self.payload["title"], exclude=DEFAULT_STRING_EXCLUDE),
            "description__exists": accept(
                self.payload["description"], exclude=DEFAULT_STRING_EXCLUDE
            ),
            "type__exists": accept(self.payload["type"], exclude=DEFAULT_STRING_EXCLUDE),
            "format__exists": accept(self.payload["format"], exclude=DEFAULT_STRING_EXCLUDE),
            "schema__exists": accept(self.payload.get("schema"), exclude=DEFAULT_JSON_EXCLUDE),
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
    checksum: Mapped[Optional[dict]] = mapped_column(JSONB)
    filesize: Mapped[Optional[str]]
    mime: Mapped[Optional[str]]
    created_at: Mapped[datetime]
    last_modified: Mapped[datetime]
    harvest: Mapped[Optional[dict]] = mapped_column(JSONB)
    internal: Mapped[dict] = mapped_column(JSONB)
    schema: Mapped[Optional[dict]] = mapped_column(JSONB)
    available: Mapped[bool]

    # indicators columns
    title__exists: Mapped[bool]
    description__exists: Mapped[Optional[bool]]
    type__exists: Mapped[Optional[bool]]
    format__exists: Mapped[Optional[bool]]
    schema__exists: Mapped[bool]

    # other computed columns
    schema__name: Mapped[str | None]

    # relationships
    dataset_id: Mapped[str] = mapped_column(ForeignKey("catalog.dataset_id"))
    dataset: Mapped["Dataset"] = relationship("Dataset", back_populates="resources")

    def __repr__(self):
        return f"<Resource {self.resource_id} of {self.dataset!r}>"

    @classmethod
    def from_payload(cls, payload: dict, dataset_id: str) -> "Resource":
        data = payload.copy()
        data["resource_id"] = data.pop("id")
        data["available"] = bool((data.get("extras") or {}).get("check:available"))

        computer = ResourceComputedColumns(data)

        return cls(
            **{
                **{k: v for k, v in data.items() if hasattr(cls, k)},
                **computer.get_computed_columns(),
                **computer.get_indicators(),
                "dataset_id": dataset_id,
            }
        )


@dataclass
class EcospheresUniverseOrganization:
    """Organization properties from our ecospheres-universe API"""

    id: str
    name: str
    slug: str
    type: str

    @classmethod
    def from_payload(cls, payload: dict) -> "EcospheresUniverseOrganization":
        return cls(
            id=payload["id"],
            name=payload["name"],
            slug=payload["slug"],
            type=payload["type"],
        )


class Organization(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(primary_key=True)
    organization_id: Mapped[str] = mapped_column(unique=True)
    name: Mapped[str]
    acronym: Mapped[Optional[str]]
    service_public: Mapped[bool]
    type: Mapped[Optional[str]]

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

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bouquet_id: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    name: Mapped[str]
    private: Mapped[bool]
    # no foreign key on organization because it's not always in the db
    organization: Mapped[Optional[str]]
    owner: Mapped[Optional[str]]
    extras: Mapped[dict] = mapped_column(JSONB)
    last_modified: Mapped[datetime]
    created_at: Mapped[datetime]
    theme: Mapped[Optional[str]]

    nb_datasets: Mapped[int]
    nb_datasets_external: Mapped[int]
    nb_factors: Mapped[int]
    nb_factors_missing: Mapped[int]
    nb_factors_not_available: Mapped[int]

    deleted: Mapped[bool]

    # relationships
    datasets: Mapped[list["Dataset"]] = relationship(
        "Dataset", secondary="datasets_bouquets", back_populates="bouquets"
    )

    def __repr__(self):
        return f"<Bouquet {self.bouquet_id}>"

    @classmethod
    def from_payload(cls, payload: dict, themes: dict[str, str]) -> "Bouquet":
        data = payload.copy()
        data["deleted"] = False

        data.pop("datasets")
        data["bouquet_id"] = data.pop("id")
        data["organization"] = data["organization"]["id"] if data["organization"] else None
        data["owner"] = data["owner"]["id"] if data["owner"] else None
        data["theme"] = next((themes[tid] for tid in themes if tid in data["tags"]), None)

        datasets_properties = data["extras"]["ecospheres"]["datasets_properties"]
        data["nb_datasets"] = len([d for d in datasets_properties if d.get("id")])
        data["nb_datasets_external"] = len(
            [d for d in datasets_properties if d.get("uri") and not d.get("id")]
        )
        data["nb_factors"] = len(datasets_properties)
        data["nb_factors_missing"] = len(
            [d for d in datasets_properties if d.get("availability") == "missing"]
        )
        data["nb_factors_not_available"] = len(
            [d for d in datasets_properties if d.get("availability") == "not available"]
        )

        return cls(**{k: v for k, v in data.items() if hasattr(cls, k)})


class DatasetBouquet(Base):
    __tablename__ = "datasets_bouquets"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bouquet_id: Mapped[str] = mapped_column(String, ForeignKey("bouquets.bouquet_id"))
    dataset_id: Mapped[str] = mapped_column(String, ForeignKey("catalog.dataset_id"))

    def __repr__(self):
        return f"<DatasetBouquet of <Bouquet {self.bouquet_id}> and <Dataset {self.dataset_id}>>"


class MetricMixin:
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[date]
    measurement: Mapped[str]
    value: Mapped[float]


class Metric(Base, MetricMixin):
    __tablename__ = "metrics"

    organization: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return f"<Metric {self.measurement}{' of ' + self.organization if self.organization else ''} at {self.date}>"


class DatasetMetric(Base, MetricMixin):
    __tablename__ = "datasets_metrics"

    dataset: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return f"<Metric {self.measurement}{' of ' + self.dataset if self.dataset else ''} at {self.date}>"


class Stats(Base):
    __tablename__ = "stats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    date: Mapped[date]
    # those attributes are directly mapped to matomo
    nb_uniq_visitors: Mapped[int]
    nb_visits: Mapped[int]
    nb_actions: Mapped[int]
    nb_visits_converted: Mapped[int]
    bounce_count: Mapped[int]
    sum_visit_length: Mapped[int]
    max_actions: Mapped[int]
    bounce_rate: Mapped[float]
    nb_actions_per_visit: Mapped[float]
    avg_time_on_site: Mapped[int]
    nb_pageviews: Mapped[int]
    nb_downloads: Mapped[int]
    nb_uniq_visitors_returning: Mapped[int]
    nb_uniq_visitors_new: Mapped[int]
