PREFIX = "/admin/v2"


async def test_unauthorized_to_admin(client, MockCurrentUser):
    async with MockCurrentUser(is_superuser=False):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 401

    async with MockCurrentUser(is_superuser=True):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200
