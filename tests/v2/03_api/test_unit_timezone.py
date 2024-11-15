from devtools import debug


async def test_timezone_api(
    client,
    db,
    MockCurrentUser,
    project_factory_v2,
):

    # authenticated
    async with MockCurrentUser() as user:
        project = await project_factory_v2(
            name="project name",
            user=user,
        )
        res = await client.get(f"/api/v2/project/{project.id}/")
        assert res.status_code == 200
        timestamp_created_db = str(project.timestamp_created)
        timestamp_created_api = res.json()["timestamp_created"]
        debug(timestamp_created_db)
        debug(timestamp_created_api)
        assert timestamp_created_api == timestamp_created_db
