from typing import Any

import pydantic_core
from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

from fractal_server.migrations.naming_convention import NAMING_CONVENTION
from fractal_server.migrations.sqltypes import AutoString


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map = {str: AutoString}

    def __init__(self, **kwargs: Any) -> None:
        """
        Table classes used to be `SQLModel`s (hence also `pydantic.BaseModel`s):
        constructing one with an unknown keyword argument was silently ignored
        (pydantic's default `extra="ignore"` behavior), and any column with a
        Python-side default (e.g. `default_factory=...`) was eagerly applied
        at construction time (pydantic always applies field defaults), rather
        than only at flush time as plain SQLAlchemy does. This constructor
        replicates both behaviors, so ORM instances built in-memory (e.g. in
        tests, or before a DB round-trip) behave the same as before.
        """
        valid_keys = {attr.key for attr in self.__mapper__.attrs}
        filtered = {k: v for k, v in kwargs.items() if k in valid_keys}
        for key, value in filtered.items():
            setattr(self, key, value)
        for col_attr in self.__mapper__.column_attrs:
            key = col_attr.key
            if key in filtered:
                continue
            default = col_attr.columns[0].default
            if default is None:
                continue
            elif default.is_scalar:
                setattr(self, key, default.arg)
            elif default.is_callable:
                setattr(self, key, default.arg(None))

    def model_dump(
        self,
        *,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> dict[str, Any]:
        """
        Dump mapped-column attributes into a dict.

        Table classes used to be `SQLModel`s (hence also `pydantic.BaseModel`s),
        and a lot of code relies on calling `.model_dump()`/`.model_dump_json()`
        directly on ORM instances. This replicates that subset of behavior:
        only mapped columns are included (never `relationship()` attributes,
        matching `SQLModel`'s exclusion of `Relationship()` fields from
        `model_dump`), and only columns that already have a concrete value in
        `__dict__` (i.e. loaded from the DB, or explicitly assigned) are
        included, matching `SQLModel`'s behavior of omitting columns whose
        value is still pending a server-side default on a not-yet-flushed
        instance.
        """
        column_names = {c.key for c in self.__mapper__.column_attrs}
        set_names = column_names & self.__dict__.keys()
        if include is not None:
            set_names &= include
        if exclude is not None:
            set_names -= exclude
        return {name: self.__dict__[name] for name in set_names}

    def model_dump_json(
        self,
        *,
        include: set[str] | None = None,
        exclude: set[str] | None = None,
    ) -> str:
        return pydantic_core.to_json(
            self.model_dump(include=include, exclude=exclude)
        ).decode()
