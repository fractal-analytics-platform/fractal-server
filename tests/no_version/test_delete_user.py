from sqlmodel import delete
from sqlmodel import select

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2


async def user_list(db):
    stm = select(UserOAuth)
    res = await db.execute(stm)
    return res.scalars().unique().all()


async def project_user_list(project_id, db):
    res = await db.execute(
        select(UserOAuth)
        .join(LinkUserProjectV2)
        .where(LinkUserProjectV2.project_id == project_id)
    )
    return res.scalars().unique().all()


async def test_delete_user(
    db,
    MockCurrentUser,
    project_factory_v2,
):
    assert len(await user_list(db)) == 0

    async with MockCurrentUser() as user:
        project1 = await project_factory_v2(user)

    async with MockCurrentUser() as user2:
        project2 = await project_factory_v2(user2)

    assert len(await user_list(db)) == 2
    assert len(await project_user_list(project1.id, db)) == 1
    assert len(await project_user_list(project2.id, db)) == 1

    await db.execute(
        delete(LinkUserProjectV2).where(LinkUserProjectV2.user_id == user.id)
    )

    await db.execute(
        delete(LinkUserGroup).where(LinkUserGroup.user_id == user.id)
    )

    await db.delete(user)
    await db.commit()

    await db.refresh(project1)
    await db.refresh(project2)

    assert len(await user_list(db)) == 1
    assert len(await project_user_list(project1.id, db)) == 0
    assert len(await project_user_list(project2.id, db)) == 1
