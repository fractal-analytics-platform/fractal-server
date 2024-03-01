from datetime import datetime
from datetime import timezone

import pytest
from devtools import debug

PREFIX = "/api/v2"


async def test_post_and_get_project(client, db, MockCurrentUser):

    PAYLOAD = dict(name="project_v2")

    # unauthenticated
    res = await client.post(f"{PREFIX}/project/", json=PAYLOAD)
    assert res.status_code == 401
    res = await client.get(f"{PREFIX}/project/")
    assert res.status_code == 401

    # authenticated
    async with MockCurrentUser(user_kwargs=dict(id=1)) as userA:
        res = await client.post(
            f"{PREFIX}/project/", json=dict(name="project")
        )
        assert res.status_code == 201
        assert len(userA.project_list) == 0
        assert len(userA.project_list_v2) == 1
        other_project = res.json()

    async with MockCurrentUser(user_kwargs=dict(id=2)) as userB:

        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert res.json() == userB.project_list_v2 == []

        res = await client.post(
            f"{PREFIX}/project/", json=dict(name="project")
        )
        assert res.status_code == 201
        assert len(userB.project_list) == 0
        assert len(userB.project_list_v2) == 1

        # a user can't create two projectsV2 with the same name
        res = await client.post(
            f"{PREFIX}/project/", json=dict(name="project")
        )
        assert res.status_code == 422
        assert len(userB.project_list_v2) == 1

        # create two V1 Projects
        for i in range(2):
            res = await client.post(
                "/api/v1/project/", json=dict(name=f"project_{i}_v1")
            )
        assert len(userB.project_list) == 2
        assert len(userB.project_list_v2) == 1

        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == userB.project_list_v2[0].id

        project_id = res.json()[0]["id"]
        res = await client.get(f"{PREFIX}/project/{project_id}/")
        assert res.status_code == 200
        assert res.json()["id"] == userB.project_list_v2[0].id
        assert (
            datetime.fromisoformat(res.json()["timestamp_created"]).tzinfo
            == timezone.utc
        )

        # fail on non existent project
        res = await client.get(f"{PREFIX}/project/123456/")
        assert res.status_code == 404

        # fail on other owner's project
        res = await client.get(f"{PREFIX}/project/{other_project['id']}/")
        assert res.status_code == 403


async def test_post_project(app, client, MockCurrentUser, db):
    payload = dict(name="new project")

    # Fail for anonymous user
    res = await client.post(f"{PREFIX}/project/", json=payload)
    data = res.json()
    assert res.status_code == 401

    async with MockCurrentUser():
        res = await client.post(f"{PREFIX}/project/", json=payload)
        data = res.json()
        assert res.status_code == 201
        debug(data)
        assert data["name"] == payload["name"]

        # Payload without `name`
        empty_payload = {}
        res = await client.post(f"{PREFIX}/project/", json=empty_payload)
        debug(res.json())
        assert res.status_code == 422


async def test_post_project_name_constraint(app, client, MockCurrentUser, db):
    payload = dict(name="new project")
    res = await client.post(f"{PREFIX}/project/", json=payload)
    assert res.status_code == 401

    async with MockCurrentUser():
        # Create a first project named "new project"
        res = await client.post(f"{PREFIX}/project/", json=payload)
        assert res.status_code == 201

        # Create a second project named "new project", and check that this
        # fails with 422_UNPROCESSABLE_ENTITY
        res = await client.post(f"{PREFIX}/project/", json=payload)
        assert res.status_code == 422


async def test_patch_project_name_constraint(app, client, MockCurrentUser, db):
    async with MockCurrentUser():
        # Create a first project named "name1"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name1"))
        assert res.status_code == 201

        # Create a second project named "name2"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name2"))
        assert res.status_code == 201
        prj2 = res.json()

        # Fail in editing the name of prj2 to "name1"
        res = await client.patch(
            f"{PREFIX}/project/{prj2['id']}/", json=dict(name="name1")
        )
        assert res.status_code == 422
        assert res.json()["detail"] == "Project name (name1) already in use"

    async with MockCurrentUser():
        # Using another user, create a project named "name3"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name3"))
        assert res.status_code == 201
        prj3 = res.json()
        # Edit the name of prj3 to "name1" without errors
        res = await client.patch(
            f"{PREFIX}/project/{prj3['id']}/", json=dict(name="name1")
        )
        debug(res.json())
        assert res.status_code == 200


@pytest.mark.parametrize("new_name", (None, "new name"))
@pytest.mark.parametrize("new_read_only", (None, True, False))
async def test_patch_project(
    new_name,
    new_read_only,
    app,
    client,
    MockCurrentUser,
    db,
):
    """
    Test that the project can be patched correctly, with any possible
    combination of set/unset attributes.
    """
    async with MockCurrentUser():
        # Create project
        payload = dict(
            name="old name",
            read_only=True,
        )
        res = await client.post(f"{PREFIX}/project/", json=payload)
        old_project = res.json()
        project_id = old_project["id"]
        assert res.status_code == 201

        # Patch project
        payload = {}
        if new_name:
            payload["name"] = new_name
        if new_read_only:
            payload["read_only"] = new_read_only
        debug(payload)
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/", json=payload
        )
        new_project = res.json()
        debug(new_project)
        assert res.status_code == 200
        for key, value in new_project.items():
            if key in payload.keys():
                assert value == payload[key]
            else:
                assert value == old_project[key]
