from sqlmodel import select

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2

PREFIX = "/auth"


async def test_no_access_user_group_api(client, registered_client):
    """
    Verify that anonymous or non-superuser users have no access to user-group
    CRUD.
    """
    for _client, expected_status in [(client, 401), (registered_client, 403)]:
        res = await _client.get(f"{PREFIX}/group/")
        assert res.status_code == expected_status

        res = await _client.post(f"{PREFIX}/group/")
        assert res.status_code == expected_status

        res = await _client.get(f"{PREFIX}/group/1/")
        assert res.status_code == expected_status

        res = await _client.patch(f"{PREFIX}/group/1/")
        assert res.status_code == expected_status

        res = await _client.delete(f"{PREFIX}/group/1/")
        assert res.status_code == expected_status


async def test_update_group(registered_superuser_client):
    """
    Modifying a group with an invalid user ID returns a 404.
    """

    # Preliminary: register a new user
    credentials_user_A = dict(email="aaa@example.org", password="12345")
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
    registered_superuser_client, db, default_user_group
):
    """
    Test basic working of POST/GET/PATCH for user groups.
    """

    # Preliminary: register two new users
    credentials_user_A = dict(email="aaa@example.org", password="12345")
    credentials_user_B = dict(email="bbb@example.org", password="12345")
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
        elif group["name"] == default_user_group.name:
            assert set(group["user_ids"]) == {user_A_id, user_B_id}
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
        f"{PREFIX}/group/{default_user_group.id}/"
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
    registered_client, registered_superuser_client
):
    """
    Test that GET-ting a single user may be enriched with group IDs/names.
    """

    # Create two groups
    GROUP_A_NAME = "my group A"
    GROUP_B_NAME = "my group B"
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name=GROUP_A_NAME)
    )
    assert res.status_code == 201
    GROUP_A_ID = res.json()["id"]
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name=GROUP_B_NAME)
    )
    assert res.status_code == 201

    # Get current user and check it has no group names/ID
    res = await registered_client.get(f"{PREFIX}/current-user/")
    assert res.status_code == 200
    current_user_id = res.json()["id"]

    # Add current user to group A
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/{GROUP_A_ID}/add-user/{current_user_id}/"
    )
    assert res.status_code == 200

    # Calls to `/auth/current-users/` may or may not include `group_names_id`,
    # depending on a query parameter
    for query_param, expected_attribute in [
        ("", None),
        ("?group_ids_names=False", None),
        ("?group_ids_names=True", [[GROUP_A_ID, GROUP_A_NAME]]),
    ]:
        res = await registered_client.get(
            f"{PREFIX}/current-user/{query_param}"
        )
        assert res.status_code == 200
        current_user = res.json()
        assert current_user["group_ids_names"] == expected_attribute

    # Calls to `/auth/users/{id}/` or may not include `group_names_id`,
    # depending on a query parameter
    for query_param, expected_attribute in [
        ("", [[GROUP_A_ID, GROUP_A_NAME]]),
        ("?group_ids_names=False", None),
        ("?group_ids_names=True", [[GROUP_A_ID, GROUP_A_NAME]]),
    ]:
        res = await registered_superuser_client.get(
            f"{PREFIX}/users/{current_user_id}/{query_param}"
        )
        assert res.status_code == 200
        user = res.json()
        assert user["group_ids_names"] == expected_attribute


async def test_patch_user_settings_bulk(
    MockCurrentUser, registered_superuser_client, default_user_group, db
):

    # Register 4 users
    async with MockCurrentUser() as user1:
        pass
    async with MockCurrentUser() as user2:
        pass
    async with MockCurrentUser() as user3:
        pass
    async with MockCurrentUser() as user4:
        pass

    user1 = await db.get(UserOAuth, user1.id)
    user2 = await db.get(UserOAuth, user2.id)
    user3 = await db.get(UserOAuth, user3.id)
    user4 = await db.get(UserOAuth, user4.id)

    for user in [user1, user2, user3, user4]:
        assert dict(
            ssh_host=None,
            ssh_username=None,
            ssh_private_key_path=None,
            ssh_tasks_dir=None,
            ssh_jobs_dir=None,
            slurm_user="test01",
            slurm_accounts=[],
            cache_dir=None,
            project_dir=None,
        ) == user.settings.dict(exclude={"id"})

    # remove user4 from default user group
    res = await db.execute(
        select(LinkUserGroup).where(LinkUserGroup.user_id == user4.id)
    )
    link = res.scalars().one()
    await db.delete(link)
    await db.commit()

    # patch user-settings of default user group
    patch = dict(
        ssh_host="127.0.0.1",
        ssh_username="fractal",
        ssh_private_key_path="/tmp/fractal",
        ssh_tasks_dir="/tmp/tasks",
        ssh_jobs_dir="/tmp/job",
        # missing `slurm_user`
        slurm_accounts=["foo", "bar"],
        cache_dir="/tmp/cache",
        project_dir="/foo",
    )
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{default_user_group.id}/user-settings/", json=patch
    )
    assert res.status_code == 200

    # assert user1, user2 and user3 has been updated
    for user in [user1, user2, user3]:
        await db.refresh(user)
        assert patch == user.settings.dict(exclude={"id", "slurm_user"})
        assert user.settings.slurm_user == "test01"  # `slurm_user` not patched
    # assert user4 has old settings
    await db.refresh(user4)
    assert dict(
        ssh_host=None,
        ssh_username=None,
        ssh_private_key_path=None,
        ssh_tasks_dir=None,
        ssh_jobs_dir=None,
        slurm_user="test01",
        slurm_accounts=[],
        cache_dir=None,
        project_dir=None,
    ) == user4.settings.dict(exclude={"id"})

    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{default_user_group.id}/user-settings/",
        json=dict(project_dir="not/an/absolute/path"),
    )
    assert res.status_code == 422

    # `None` is a valid `project_dir`
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{default_user_group.id}/user-settings/",
        json=dict(project_dir="/fancy/dir"),
    )
    assert res.status_code == 200
    for user in [user1, user2, user3]:
        await db.refresh(user)
        assert user.settings.project_dir == "/fancy/dir"
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{default_user_group.id}/user-settings/",
        json=dict(project_dir=None),
    )
    for user in [user1, user2, user3]:
        await db.refresh(user)
        assert user.settings.project_dir is None
