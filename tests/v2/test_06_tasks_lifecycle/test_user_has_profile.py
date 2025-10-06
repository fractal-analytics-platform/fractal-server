import pytest
from fastapi import HTTPException

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.routes.aux.validate_user_profile import (
    user_has_profile_or_422,
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
        email="example@example.org",
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
