"""
These tests are related to issue
https://github.com/fractal-analytics-platform/fractal-server/issues/2449
"""
from enum import StrEnum

from sqlmodel import select

from fractal_server.app.models.v2 import ProjectV2


class FakeEnum(StrEnum):
    FOO = "foo"
    BAR = "bar"


async def test_unit_enum_in_db_queries(db, local_resource_profile_db):
    resource, _ = local_resource_profile_db
    db.add_all(
        [
            ProjectV2(name="foo", resource_id=resource.id),
            ProjectV2(name="foo", resource_id=resource.id),
            ProjectV2(name="bar", resource_id=resource.id),
            ProjectV2(name="name", resource_id=resource.id),
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
