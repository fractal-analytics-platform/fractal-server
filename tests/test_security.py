from devtools import debug


PREFIX = "/auth"


async def test_me_unkonwn(client):
    # Anonymous
    res = await client.get(f"{PREFIX}/users/me")
    debug(res.json())
    assert res.status_code == 401


async def test_me_known(registered_client):
    res = await registered_client.get(f"{PREFIX}/users/me")
    debug(res.json())
    assert res.status_code == 200
