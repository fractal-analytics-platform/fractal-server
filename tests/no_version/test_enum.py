import sys
from enum import Enum

from sqlmodel import select

from fractal_server.app.models.v2 import ProjectV2


class FakeEnum(str, Enum):
    FOO = "foo"
    BAR = "bar"


async def test_unit_enum_in_db_queries(db):
    db.add_all(
        [
            ProjectV2(name="foo"),
            ProjectV2(name="foo"),
            ProjectV2(name="bar"),
            ProjectV2(name="name"),
        ]
    )
    await db.commit()

    res = await db.execute(
        select(ProjectV2).where(ProjectV2.name == FakeEnum.FOO)
    )
    assert len(res.scalars().all()) == 2
    res = await db.execute(
        select(ProjectV2).where(ProjectV2.name != FakeEnum.FOO)
    )
    assert len(res.scalars().all()) == 2

    res = await db.execute(
        select(ProjectV2).where(ProjectV2.name == FakeEnum.BAR)
    )
    assert len(res.scalars().all()) == 1
    res = await db.execute(
        select(ProjectV2).where(ProjectV2.name != FakeEnum.BAR)
    )
    assert len(res.scalars().all()) == 3


async def test_unit_enum_comparison():

    assert FakeEnum.FOO == "foo"
    assert FakeEnum.FOO.value == "foo"
    assert f"{FakeEnum.FOO.value}" == "foo"

    if sys.version_info.minor < 11:
        assert f"{FakeEnum.FOO}" == "foo"
    else:
        assert f"{FakeEnum.FOO}" == "FakeEnum.FOO"
