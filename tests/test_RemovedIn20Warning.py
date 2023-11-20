import pytest
from sqlalchemy import column
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy import table


@pytest.mark.skip()
def test_RemovedIn20Warnings():
    """
    This is a dummy test, to make sure that we are catching the
    RemovedIn20Warnings warning during the migration towards SQLAlchemy 2 - see
    example in the docs:
    https://docs.sqlalchemy.org/en/20/changelog/migration_20.html#migration-to-2-0-step-two-turn-on-removedin20warnings
    """

    engine = create_engine("sqlite://")
    engine.execute("CREATE TABLE foo (id integer)")
    engine.execute("INSERT INTO foo (id) VALUES (1)")
    foo = table("foo", column("id"))
    result = engine.execute(select([foo.c.id]))
    print(result.fetchall())
