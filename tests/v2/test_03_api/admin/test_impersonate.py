from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from fractal_server.app.models.security import UserOAuth

PREFIX = "/admin/v2"


async def test_impersonate(
    app: FastAPI,
    client,
    MockCurrentUser,
    db,
    registered_superuser_client,
):
    # Create user u1
    u1 = UserOAuth(
        email="user1@email.com",
        hashed_password="abc1",
        project_dir="/fake",
    )
    db.add(u1)
    await db.commit()
    await db.refresh(u1)

    # Get token to impoersonate user u1
    res = await registered_superuser_client.get(
        f"{PREFIX}/impersonate/{u1.id}/"
    )
    assert res.status_code == 200
    token_impersonate = res.json()["access_token"]

    # Impersonate user u1
    async with AsyncClient(
        base_url="http://test",
        transport=ASGITransport(app=app),
    ) as client_impersonate:
        res = await client_impersonate.get(
            "/auth/current-user/",
            headers={"Authorization": f"Bearer {token_impersonate}"},
        )
        assert res.json()["email"] == u1.email

    # Try to impersonate an invalid user
    res = await registered_superuser_client.get(f"{PREFIX}/impersonate/999/")
    assert res.status_code == 404

    # Try to impersonate a user without being a superuser
    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        res = await client.get(f"{PREFIX}/impersonate/{u1.id}/")
        assert res.status_code == 401
