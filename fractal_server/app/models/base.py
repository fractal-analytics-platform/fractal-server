import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import MetaData
from sqlalchemy import inspect
from sqlalchemy.orm import DeclarativeBase

from fractal_server.migrations.naming_convention import NAMING_CONVENTION


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _json_default(obj: Any) -> str:
    """
    `json.dumps(..., default=...)` hook covering the only non-JSON-native
    types that can end up nested in a `dict[str, Any]`/JSON column.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, Path):
        return obj.as_posix()
    elif isinstance(obj, UUID):
        return str(obj)
    raise TypeError(
        f"Object of type {type(obj).__name__} is not JSON serializable"
    )


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=_json_default)


def dump_model(
    obj: Base,
    *,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
) -> dict[str, Any]:
    """
    Dump mapped-column attributes into a dict.

    A lot of code relies on calling `.model_dump()`/`.model_dump_json()`
    directly on ORM instances, mirroring their former pydantic-based
    behavior: only mapped columns are included (never `relationship()`
    attributes), and only columns that already have a concrete value in
    `__dict__` (i.e. loaded from the DB, or explicitly assigned) are
    included, omitting columns whose value is still pending a
    server-side default on a not-yet-flushed instance.
    """
    column_names = {c.key for c in inspect(obj).mapper.column_attrs}
    set_names = column_names & obj.__dict__.keys()
    if include is not None:
        set_names &= include
    if exclude is not None:
        set_names -= exclude
    return {name: obj.__dict__[name] for name in set_names}


def dump_model_to_json(
    obj: Base,
    *,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
) -> str:
    dumped = dump_model(obj, include=include, exclude=exclude)
    return json_dumps(dumped)
