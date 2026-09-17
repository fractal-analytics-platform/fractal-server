from typing import Any

from sqlalchemy import MetaData
from sqlalchemy import inspect
from sqlalchemy.orm import DeclarativeBase

from fractal_server.migrations.naming_convention import NAMING_CONVENTION


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def orm_model_to_dict(
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
