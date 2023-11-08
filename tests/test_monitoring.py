PREFIX = "api/v1"


async def test_unauthorized_to_monitor(registered_client):

    res = await registered_client.get(f"{PREFIX}/monitoring/project/")
    assert res.status_code == 403


async def test_monitor_project(client, MockCurrentUser, project_factory):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        res = await client.get(f"{PREFIX}/monitoring/project/")
        assert res.status_code == 200
        assert res.json() == []

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:
        prj1 = await project_factory(user)
        await project_factory(user)
        prj1_id = prj1.id
        res = await client.get(f"{PREFIX}/monitoring/project/")
        assert res.status_code == 403

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        res = await client.get(f"{PREFIX}/monitoring/project/")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/monitoring/project/?id={prj1_id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
