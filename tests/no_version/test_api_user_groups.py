from sqlmodel import select

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models.v2 import TaskGroupV2
from tests.fixtures_server import PROJECT_DIR_PLACEHOLDER

PREFIX = "/auth"


async def test_no_access_user_group_api(client):
    """
    Verify that anonymous users have no access to user-group CRUD.
    """
    expected_status = 401
    res = await client.get(f"{PREFIX}/group/")
    assert res.status_code == expected_status
    res = await client.post(f"{PREFIX}/group/")
    assert res.status_code == expected_status
    res = await client.get(f"{PREFIX}/group/1/")
    assert res.status_code == expected_status
    res = await client.patch(f"{PREFIX}/group/1/")
    assert res.status_code == expected_status
    res = await client.delete(f"{PREFIX}/group/1/")
    assert res.status_code == expected_status


async def test_update_group(registered_superuser_client):
    """
    Modifying a group with an invalid user ID returns a 404.
    """

    # Preliminary: register a new user
    credentials_user_A = dict(
        email="aaa@example.org",
        password="12345",
        project_dir=PROJECT_DIR_PLACEHOLDER,
    )
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=credentials_user_A
    )
    assert res.status_code == 201
    user_A_id = res.json()["id"]

    # Preliminary: create a group
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name="group1", viewer_paths=["/old"])
    )
    assert res.status_code == 201
    group_data = res.json()
    group_id = group_data["id"]
    assert group_data["user_ids"] == []
    assert group_data["viewer_paths"] == ["/old"]

    invalid_id = 99999
    # Path a non existing group
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{invalid_id}/add-user/{user_A_id}/"
    )
    assert res.status_code == 404

    # Check that group was not updated
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_id}/",
    )
    assert res.status_code == 200
    assert res.json()["user_ids"] == []

    # Patch an existing group by adding a valid user
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{group_id}/add-user/{user_A_id}/",
    )
    assert res.status_code == 200
    assert res.json()["user_ids"] == [user_A_id]

    # Patch an existing group by replacing `viewer_paths` with a new list
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_id}/",
        json=dict(viewer_paths=["/new"]),
    )
    assert res.status_code == 200
    assert res.json()["viewer_paths"] == ["/new"]


async def test_user_group_crud(
    registered_superuser_client,
    db,
    create_default_group,
    local_resource_profile_db,
):
    """
    Test basic working of POST/GET/PATCH for user groups.
    """

    # Preliminary: register two new users
    credentials_user_A = dict(
        email="aaa@example.org",
        password="12345",
        project_dir=PROJECT_DIR_PLACEHOLDER,
    )
    credentials_user_B = dict(
        email="bbb@example.org",
        password="12345",
        project_dir=PROJECT_DIR_PLACEHOLDER,
    )
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=credentials_user_A
    )
    assert res.status_code == 201
    user_A_id = res.json()["id"]
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=credentials_user_B
    )
    assert res.status_code == 201
    user_B_id = res.json()["id"]

    # Create group 1 with user A
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name="group 1")
    )
    assert res.status_code == 201
    group_1_id = res.json()["id"]

    # Create group 2 with users A and B
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/",
        json=dict(name="group 2"),
    )
    assert res.status_code == 201
    group_2_id = res.json()["id"]

    # Add user A and B to group 1
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{group_1_id}/add-user/{user_A_id}/"
    )
    assert res.status_code == 200
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{group_1_id}/add-user/{user_B_id}/"
    )
    assert res.status_code == 200
    # Add user B to group 2
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{group_2_id}/add-user/{user_B_id}/"
    )
    assert res.status_code == 200

    # Get all groups (group 1, group 2)
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/?user_ids=true"
    )
    assert res.status_code == 200
    groups_data = res.json()
    assert len(groups_data) == 3
    for group in groups_data:
        if group["name"] == "group 1":
            assert set(group["user_ids"]) == {user_A_id, user_B_id}
        elif group["name"] == "group 2":
            assert group["user_ids"] == [user_B_id]
        elif group["name"] == create_default_group.name:
            assert set(group["user_ids"]) == {
                create_default_group.id,
                user_A_id,
                user_B_id,
            }
        else:
            raise RuntimeError("Wrong branch.")

    # Get all groups (group 1, group 2) without user_ids
    res = await registered_superuser_client.get(f"{PREFIX}/group/")
    assert res.status_code == 200
    groups_data = res.json()
    assert len(groups_data) == 3
    for group in groups_data:
        assert group["user_ids"] is None

    # Add users B to group 2, and fail because user B is already there
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{group_2_id}/add-user/{user_B_id}/"
    )
    assert res.status_code == 422
    assert "is already a member" in res.json()["detail"]

    # After the previous 422, verify that user A was not added to group 2
    # (that is, verify that `db.commit` is atomic)
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_2_id}/"
    )
    assert res.status_code == 200
    assert user_A_id not in res.json()["user_ids"]

    # Try to remove user from 'All' group and fail
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{create_default_group.id}/remove-user/{user_A_id}/"
    )
    assert res.status_code == 422
    assert "Cannot remove user from default user group" in str(
        res.json()["detail"]
    )

    # Remove users B from group 2, twice
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{group_2_id}/remove-user/{user_B_id}/"
    )
    assert res.status_code == 200
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{group_2_id}/remove-user/{user_B_id}/"
    )
    assert res.status_code == 422
    assert "is not a member" in res.json()["detail"]

    # DELETE (and cascade operations)
    task_group = TaskGroupV2(
        user_id=user_A_id,
        user_group_id=group_1_id,
        origin="pypi",
        pkg_name="fractal-tasks-core",
        resource_id=local_resource_profile_db[0].id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    assert task_group.user_group_id == group_1_id

    res = await registered_superuser_client.delete(  # actual DELETE
        f"{PREFIX}/group/{group_1_id}/"
    )
    assert res.status_code == 204
    res = await registered_superuser_client.delete(
        f"{PREFIX}/group/{group_1_id}/"
    )
    assert res.status_code == 404
    res = await registered_superuser_client.delete(
        f"{PREFIX}/group/{create_default_group.id}/"
    )
    assert res.status_code == 422

    # test cascade operations
    res = await db.execute(
        select(LinkUserGroup).where(LinkUserGroup.group_id == group_1_id)
    )
    links = res.scalars().all()
    assert links == []
    await db.refresh(task_group)
    assert task_group.user_group_id is None


async def test_create_user_group_same_name(registered_superuser_client):
    """
    Test that you cannot create two groups with the same name.
    """
    # First group creation
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name="mygroup")
    )
    assert res.status_code == 201
    # Second group creation
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name="mygroup")
    )
    assert res.status_code == 422
    assert "A group with the same name already exists" in str(res.json())


async def test_get_user_optional_group_info(
    MockCurrentUser, client, create_default_group
):
    """
    Test that GET-ting a single user may be enriched with group IDs/names.
    """
    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True)
    ) as superuser:
        superuser_id = superuser.id
        # Create two groups
        GROUP_A_NAME = "my group A"
        GROUP_B_NAME = "my group B"
        res = await client.post(
            f"{PREFIX}/group/", json=dict(name=GROUP_A_NAME)
        )
        assert res.status_code == 201
        GROUP_A_ID = res.json()["id"]
        res = await client.post(
            f"{PREFIX}/group/", json=dict(name=GROUP_B_NAME)
        )
        assert res.status_code == 201

    # Get current user and check it has no group names/ID
    async with MockCurrentUser() as user:
        user_id = user.id
        res = await client.get(f"{PREFIX}/current-user/")
        assert res.status_code == 200
        current_user_id = res.json()["id"]

    # Add current user to group A
    async with MockCurrentUser(user_kwargs=dict(id=superuser_id)):
        res = await client.post(
            f"{PREFIX}/group/{GROUP_A_ID}/add-user/{current_user_id}/"
        )
        assert res.status_code == 200

    # Calls to `/auth/current-users/` may or may not include `group_names_id`,
    # depending on a query parameter
    async with MockCurrentUser(user_kwargs=dict(id=user_id)):
        for query_param, expected_attribute in [
            ("", None),
            ("?group_ids_names=False", None),
            (
                "?group_ids_names=True",
                [
                    [create_default_group.id, create_default_group.name],
                    [GROUP_A_ID, GROUP_A_NAME],
                ],
            ),
        ]:
            res = await client.get(f"{PREFIX}/current-user/{query_param}")
            assert res.status_code == 200
            current_user = res.json()
            assert current_user["group_ids_names"] == expected_attribute

    # Calls to `/auth/users/{id}/` or may not include `group_names_id`,
    # depending on a query parameter
    async with MockCurrentUser(user_kwargs=dict(id=superuser_id)):
        for query_param, expected_attribute in [
            (
                "",
                [
                    [create_default_group.id, create_default_group.name],
                    [GROUP_A_ID, GROUP_A_NAME],
                ],
            ),
            ("?group_ids_names=False", None),
            (
                "?group_ids_names=True",
                [
                    [create_default_group.id, create_default_group.name],
                    [GROUP_A_ID, GROUP_A_NAME],
                ],
            ),
        ]:
            res = await client.get(
                f"{PREFIX}/users/{current_user_id}/{query_param}"
            )
            assert res.status_code == 200
            user = res.json()
            assert user["group_ids_names"] == expected_attribute
