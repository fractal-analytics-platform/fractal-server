from sqlmodel import select

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.security import _create_first_user


async def count_users(db):
    res = await db.execute(select(UserOAuth))
    return len(res.unique().scalars().all())


async def test_unit_create_first_user(db):

    assert await count_users(db) == 0

    await _create_first_user(email="test1@fractal.com", password="xxxx")
    assert await count_users(db) == 1

    await _create_first_user(
        email="test2@fractal.com", password="xxxx", username="test2"
    )
    assert await count_users(db) == 2

    # UserAlreadyExists
    await _create_first_user(email="test2@fractal.com", password="xxxx")
    assert await count_users(db) == 2
    # FIXME test line 322:
    # logger.warning(f"User {email} already exists")

    await _create_first_user(
        email="test3@fractal.com", password="xxxx", is_superuser=True
    )
    assert await count_users(db) == 3
    # can't create more than one superuser
    await _create_first_user(
        email="test4@fractal.com", password="xxxx", is_superuser=True
    )
    assert await count_users(db) == 3
    # FIXME test lines 296-299:
    # logger.info(
    #     f"{existing_superuser.email} superuser already exists,"
    #     f" skip creation of {email}"
    # )

    # FIXME test lines 315-319:
    # except IntegrityError:
    #     logger.warning(
    #         f"Creation of user {email} failed with IntegrityError "
    #         "(likely due to concurrent attempts from different workers)."
    #     )
