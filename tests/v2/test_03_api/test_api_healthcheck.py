from devtools import debug


async def test_healthcheck(client, db, MockCurrentUser):

    async with MockCurrentUser() as userA:
        res = await client.post("/api/v2/healthcheck/")
        debug(res)
        assert res.status_code == 200