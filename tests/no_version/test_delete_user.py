from sqlmodel import delete
from sqlmodel import select

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.security import UserSettings
from fractal_server.app.models.v1 import LinkUserProject
from fractal_server.app.models.v2 import LinkUserProjectV2


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
        project_v1 = await project_factory(user)
        project_v2 = await project_factory_v2(user)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user2:
        project_v1_2 = await project_factory(user2)
        project_v2_2 = await project_factory_v2(user2)

    assert len(await user_list(db)) == 2
    assert len(project_v1.user_list) == 1
    assert len(project_v2.user_list) == 1
    assert len(project_v1_2.user_list) == 1
    assert len(project_v2_2.user_list) == 1

    await db.execute(
        delete(LinkUserProject).where(LinkUserProject.user_id == user.id)
    )
    await db.execute(
        delete(LinkUserProjectV2).where(LinkUserProjectV2.user_id == user.id)
    )

    await db.execute(
        delete(LinkUserGroup).where(LinkUserGroup.user_id == user.id)
    )

    await db.delete(user)
    await db.commit()

    await db.refresh(project_v1)
    await db.refresh(project_v2)
    await db.refresh(project_v1_2)
    await db.refresh(project_v2_2)

    assert len(await user_list(db)) == 1
    assert len(project_v1.user_list) == 0
    assert len(project_v2.user_list) == 0
    assert len(project_v1_2.user_list) == 1
    assert len(project_v2_2.user_list) == 1


async def test_cascade_on_delete_user_settings(db):

    user = UserOAuth(
        email="user@fractal.xy",
        hashed_password="fake_hashed_password",
        settings=UserSettings(slurm_accounts=["account1", "account2"]),
    )
    db.add(user)
    await db.commit()

    await db.refresh(user)
    user_id = user.id
    user_settings_id = user.user_settings_id

    await db.close()

    user = await db.get(UserOAuth, user_id)
    assert user is not None
    user_settings = await db.get(UserSettings, user_settings_id)
    assert user_settings is not None

    await db.delete(user)
    await db.commit()

    user = await db.get(UserOAuth, user_id)
    assert user is None
    user_settings = await db.get(UserSettings, user_settings_id)
    assert user_settings is None
