import pytest
from devtools import debug
from fastapi import HTTPException

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.routes.aux.validate_user_profile import (
    user_has_profile_or_422,
)
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)


async def test_user_has_profile_or_422(
    db,
    local_resource_profile_db,
):
    # Failure
    class MockUser:
        profile_id: int | None = None
        email: str = "user@example.org"

    with pytest.raises(HTTPException, match="422"):
        await user_has_profile_or_422(user=MockUser())

    # Success
    user = UserOAuth(
        email="user@example.org",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        hashed_password="fake",
        profile_id=local_resource_profile_db[1].id,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    await user_has_profile_or_422(user=user)


async def test_validate_user_profile_local(
    db,
    MockCurrentUser,
    local_resource_profile_db,
):
    async with MockCurrentUser(
        user_kwargs=dict(profile_id=local_resource_profile_db[1].id)
    ) as user:
        debug(user)
        res, prof = await validate_user_profile(user=user, db=db)
        debug(res)
