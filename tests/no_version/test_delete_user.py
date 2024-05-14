import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v1 import LinkUserProject
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


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
    settings = Inject(get_settings)
    assert len(await user_list(db)) == 0

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        assert len(await user_list(db)) == 1
        project_v1 = await project_factory(user)
        project_v2 = await project_factory_v2(user)

    assert len(await user_list(db)) == 1
    assert len(project_v1.user_list) == 1
    assert len(project_v2.user_list) == 1

    await db.delete(user)

    if settings.DB_ENGINE == "postgres":
        with pytest.raises(IntegrityError):
            # still referred in LinkUserProject and LinkUserProjectV2
            await db.commit()
        await db.rollback()
    else:
        await db.commit()

    await db.refresh(project_v1)
    await db.refresh(project_v2)

    if settings.DB_ENGINE == "postgres":
        assert len(await user_list(db)) == 1
        assert len(project_v1.user_list) == 1
        assert len(project_v2.user_list) == 1
    else:
        assert len(await user_list(db)) == 0
        assert len(project_v1.user_list) == 0
        assert len(project_v2.user_list) == 0

    link_v1_list = (
        (
            await db.execute(
                select(LinkUserProject).where(
                    LinkUserProject.user_id == user.id
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(link_v1_list) == 1

    link_v2_list = (
        (
            await db.execute(
                select(LinkUserProjectV2).where(
                    LinkUserProjectV2.user_id == user.id
                )
            )
        )
        .scalars()
        .all()
    )
    assert len(link_v2_list) == 1

    for link in link_v1_list + link_v2_list:
        await db.delete(link)
        await db.commit()

    if settings.DB_ENGINE == "postgres":
        await db.delete(user)
        # not referred anymore in LinkUserProject and LinkUserProjectV2
        await db.commit()
        await db.refresh(project_v1)
        await db.refresh(project_v2)

        assert len(await user_list(db)) == 0
        assert len(project_v1.user_list) == 0
        assert len(project_v2.user_list) == 0
