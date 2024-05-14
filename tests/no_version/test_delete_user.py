from sqlmodel import select

from fractal_server.app.models.security import UserOAuth


async def user_list(db):
    stm = select(UserOAuth)
    res = await db.execute(stm)
    return res.scalars().unique().all()


async def test_delete_user(
    db,
    MockCurrentUser,
    project_factory,
    project_factory_v2,
):
    assert len(await user_list(db)) == 0

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        assert len(await user_list(db)) == 1
        project_v1 = await project_factory(user)
        project_v2 = await project_factory_v2(user)

    assert len(await user_list(db)) == 1
    assert len(project_v1.user_list) == 1
    assert len(project_v2.user_list) == 1

    await db.delete(user)
    await db.commit()
    await db.refresh(project_v1)
    await db.refresh(project_v2)

    assert len(await user_list(db)) == 0
    assert len(project_v1.user_list) == 0
    assert len(project_v2.user_list) == 0
