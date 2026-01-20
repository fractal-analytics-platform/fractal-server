from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_user_act
from fractal_server.app.routes.auth import get_api_guest
from fractal_server.app.security import _create_first_user

_EMAIL = "test@example.org"
_PWD = "12345"


async def test_get_api_user_and_guest(
    app: FastAPI,
    client,
    local_resource_profile_db,
    db,
):
    await _create_first_user(
        email=_EMAIL,
        password=_PWD,
        is_superuser=False,
        is_verified=True,
        project_dir="/fake",
    )
    async with AsyncClient(
        base_url="http://test",
        transport=ASGITransport(app=app),
    ) as client:
        # Get token
        data_login = dict(username=_EMAIL, password=_PWD)
        res = await client.post("auth/token/login/", data=data_login)
        token = res.json()["access_token"]

        # Set Authorization header, so that the client impersonates this user
        client.headers["Authorization"] = f"Bearer {token}"

        # Success in GET-current-user (which depends on `current_user_act`)
        res = await client.get("/auth/current-user/")
        assert res.status_code == 200
        user_id = res.json()["id"]
        assert res.json()["email"] == _EMAIL
        assert res.json()["profile_id"] is None

        assert app.dependency_overrides == {}
        app.dependency_overrides[current_user_act] = get_api_guest

        # Failure in GET-current-user, if it provisionally depends on
        # `get_api_guest`
        res = await client.get("/auth/current-user/")
        assert res.status_code == 403
        assert res.json()["detail"] == (
            "Forbidden access (user.is_verified=True user.profile_id=None)."
        )

        # Set user.profile_id
        _, profile = local_resource_profile_db
        user = await db.get(UserOAuth, user_id)
        user.profile_id = profile.id
        db.add(user)
        await db.commit()
        await db.close()

        res = await client.get("/auth/current-user/")
        assert res.status_code == 200

        app.dependency_overrides = {}
