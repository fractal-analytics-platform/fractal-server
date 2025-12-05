import pytest
from fastapi import HTTPException

from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v2 import Resource
from fractal_server.app.routes.aux.validate_user_profile import (
    user_has_profile_or_422,
)
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)
from fractal_server.app.schemas.v2.resource import cast_serialize_pixi_settings


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
        project_dirs=["/fake"],
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
    res, prof = local_resource_profile_db
    res.tasks_pixi_config = cast_serialize_pixi_settings(
        {
            "default_version": "0.41.0",
            "versions": {"0.41.0": "/common/path/pixi/0.41.0/"},
        }
    )
    db.add(res)
    await db.commit()
    await db.refresh(res)

    async with MockCurrentUser(profile_id=prof.id) as u:
        # Successful validation
        _res, _prof = await validate_user_profile(user=u, db=db)
        assert _res.tasks_pixi_config["TOKIO_WORKER_THREADS"]

        assert _res.id == res.id
        assert _prof.id == prof.id

        # # Failed validation
        current_res = await db.get(Resource, res.id)
        current_res.tasks_python_config = {"invalid": "data"}
        db.add(current_res)
        await db.commit()
        with pytest.raises(HTTPException, match="are not valid"):
            await validate_user_profile(user=u, db=db)
