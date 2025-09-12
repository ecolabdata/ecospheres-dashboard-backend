from collections.abc import Sequence
from typing import Any, Protocol

from sqlalchemy.orm import scoped_session


def no_value_dict(obj: Any) -> bool:
    return isinstance(obj, dict) and all(v is None for v in obj.values())


DEFAULT_EXCLUDE = (None,)
DEFAULT_JSON_EXCLUDE = (None, {}, no_value_dict)
DEFAULT_LIST_EXCLUDE = (None, [])
DEFAULT_STRING_EXCLUDE = (None, "")


def accept(element: Any, exclude: Sequence[Any] = DEFAULT_EXCLUDE) -> bool:
    """
    Return True if `element` is not in the `exclude` sequence, False otherwise.

    The `exclude` sequence can contain:
    - A value which should be excluded.
    - A callable taking `element` as a single parameter and returning True iff `element` should be excluded.
    """
    for item in exclude:
        if callable(item) and item(element):
            return False
        elif element == item:
            return False
    return True


class HasId(Protocol):
    id: Any


def upsert[T: HasId](
    session: scoped_session, new: T, existing: T | None, auto_commit: bool = True
) -> T:
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
