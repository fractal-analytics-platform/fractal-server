from sqlalchemy import String
from sqlalchemy.types import TypeDecorator


class AutoString(TypeDecorator):
    """
    A plain `String`, wrapped in a `TypeDecorator`.

    A `TypeDecorator`-wrapped column keeps its own type when compared
    against a mismatched-type literal (e.g. `OAuthAccount.account_id == 1`,
    `account_id` being a `str` column), whereas a plain `String` column lets
    SQLAlchemy infer the bind parameter's type from the literal instead
    (`INTEGER` for a Python `int`), which then fails at the DB level
    (`character varying = integer`).

    Historical migrations (`fractal_server/migrations/versions/`) use this
    as the column type for every plain string column, mirroring what
    `sqlmodel.sql.sqltypes.AutoString` used to generate when the ORM layer
    was based on `SQLModel`. `fractal_server/app/models/base.py` registers
    it as the default type for bare `str` annotations for the same reason.
    """

    impl = String
    cache_ok = True
