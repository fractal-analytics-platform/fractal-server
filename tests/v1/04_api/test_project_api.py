from datetime import datetime
from datetime import timezone

from devtools import debug


PREFIX = "/api/v1"


async def test_get_project(client, db, project_factory, MockCurrentUser):
    # unauthenticated
    res = await client.get(f"{PREFIX}/project/")
    assert res.status_code == 401

    # authenticated
    async with MockCurrentUser() as user:
        other_project = await project_factory(user)

    async with MockCurrentUser() as user:
        res = await client.get(f"{PREFIX}/project/")
        debug(res)
        assert res.status_code == 200
        assert res.json() == []

        await project_factory(user)
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        debug(data)
        assert res.status_code == 200
        assert len(data) == 1

        project_id = data[0]["id"]
        res = await client.get(f"{PREFIX}/project/{project_id}/")
        assert res.status_code == 200
        assert res.json()["id"] == project_id
        assert (
            datetime.fromisoformat(res.json()["timestamp_created"]).tzinfo
            == timezone.utc
        )

        # fail on non existent project
        res = await client.get(f"{PREFIX}/project/123456/")
        assert res.status_code == 404

        # fail on other owner's project
        res = await client.get(f"{PREFIX}/project/{other_project.id}/")
        assert res.status_code == 403
