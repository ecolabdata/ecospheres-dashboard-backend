from typing import TypeAlias, TypeVar

from sqlalchemy.orm import scoped_session

from models import Bouquet, Dataset, Metric, Organization, Resource, Stats

Model: TypeAlias = Bouquet | Dataset | Metric | Organization | Resource | Stats
T = TypeVar("T", bound=Model)


def upsert(session: scoped_session, new: T, existing: T | None, auto_commit: bool = True) -> T:
    if existing:
        new.id = existing.id
        session.merge(new)
        result = existing
    else:
        session.add(new)
        result = new
    # creates the id if needed
    session.flush()
    if auto_commit:
        session.commit()
    return result
