from fractal_server.app.models.security import UserOAuth


PREFIX = "/admin/v2"


async def test_impersonate(
    client, MockCurrentUser, db, registered_superuser_client
):
    u1 = UserOAuth(email="user1@email.com", hashed_password="abc1")
    db.add(u1)
    await db.commit()
    await db.refresh(u1)

    res = await registered_superuser_client.get(
        f"{PREFIX}/impersonate/{u1.id}"
    )
    assert res.status_code == 200

    res = await registered_superuser_client.get(f"{PREFIX}/impersonate/")
    assert res.status_code == 404

    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        res = await client.get(f"{PREFIX}/impersonate/{u1.id}")
        assert res.status_code == 401
