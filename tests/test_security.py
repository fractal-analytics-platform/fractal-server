from devtools import debug


PREFIX = "/auth"


async def test_me_unkonwn(client):
    # Anonymous
    res = await client.get(f"{PREFIX}/users/me")
    debug(res.json())
    assert res.status_code == 401


async def test_me_known(client):
    data_register = dict(
        email="test@test.com", slurm_user="test", password="123"
    )

    data_login = dict(
        username="test@test.com", password="123", lurm_user="test"
    )
    res = await client.post(f"{PREFIX}/register", json=data_register)
    res = await client.post(f"{PREFIX}/token/login", data=data_login)
    token = res.json()["access_token"]
    client.headers["Authorization"] = f"Bearer {token}"
    res = await client.get(f"{PREFIX}/users/me")
    debug(res.json())
    assert res.status_code == 200
