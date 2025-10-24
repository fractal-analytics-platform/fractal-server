from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from fractal_server.app.routes.auth import current_user_act
from fractal_server.app.routes.auth import current_user_act_ver_prof
from fractal_server.app.security import _create_first_user

_EMAIL = "test@test.com"
_PWD = "12345"


async def test_current_user_act_ver_prof(app: FastAPI, client):
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
        assert res.json()["email"] == _EMAIL
        assert res.json()["profile_id"] is None

        # Failure in GET-current-user, if it provisionally depends on
        # `current_user_act_ver_prof`
        assert app.dependency_overrides == {}
        app.dependency_overrides[current_user_act] = current_user_act_ver_prof
        res = await client.get("/auth/current-user/")
        assert res.status_code == 403
        assert res.json()["detail"] == (
            "Forbidden access (user.is_verified=True user.profile_id=None)."
        )
        app.dependency_overrides = {}
