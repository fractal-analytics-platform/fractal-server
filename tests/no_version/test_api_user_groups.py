from devtools import debug

# import pytest

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


async def test_delete_user_group_not_allowed(registered_superuser_client):
    """
    Verify that the user-group DELETE endpoint responds with
    "405 Method Not Allowed".
    """
    res = await registered_superuser_client.delete(f"{PREFIX}/group/1/")
    assert res.status_code == 405


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
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{invalid_id}/",
        json=dict(new_user_ids=[user_A_id]),
    )
    assert res.status_code == 404
    # Patch an existing group by adding both valid and invalid users
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_id}/",
        json=dict(new_user_ids=[user_A_id, invalid_id]),
    )
    assert res.status_code == 404

    # Check that group was not updated
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_id}/",
    )
    assert res.status_code == 200
    assert res.json()["user_ids"] == []

    # Patch an existing group by adding a valid user
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_id}/",
        json=dict(new_user_ids=[user_A_id]),
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


async def test_user_group_crud(registered_superuser_client):
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

    # Add user A to group 1
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_1_id}/",
        json=dict(new_user_ids=[user_A_id, user_B_id]),
    )
    assert res.status_code == 200
    # Add user B to group 2
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_2_id}/", json=dict(new_user_ids=[user_B_id])
    )
    assert res.status_code == 200

    # Get all groups (group 1, group 2)
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/?user_ids=true"
    )
    assert res.status_code == 200
    groups_data = res.json()
    assert len(groups_data) == 2
    for group in groups_data:
        if group["name"] == "group 1":
            assert set(group["user_ids"]) == {user_A_id, user_B_id}
        elif group["name"] == "group 2":
            assert group["user_ids"] == [user_B_id]
        else:
            raise RuntimeError("Wrong branch.")

    # Get all groups (group 1, group 2) without user_ids
    res = await registered_superuser_client.get(f"{PREFIX}/group/")
    assert res.status_code == 200
    groups_data = res.json()
    assert len(groups_data) == 2
    for group in groups_data:
        assert group["user_ids"] is None

    # Add users A and B to group 2, and fail because user B is already there
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_2_id}/",
        json=dict(new_user_ids=[user_A_id, user_B_id]),
    )
    assert res.status_code == 422
    hint_msg = "Likely reason: one of these links already exists"
    assert hint_msg in res.json()["detail"]

    # After the previous 422, verify that user A was not added to group 2
    # (that is, verify that `db.commit` is atomic)
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_2_id}/"
    )
    assert res.status_code == 200
    assert user_A_id not in res.json()["user_ids"]

    # Create user/group link and fail because of repeated IDs
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_1_id}/",
        json=dict(new_user_ids=[99, 99]),
    )
    assert res.status_code == 422
    assert "`new_user_ids` list has repetitions'" in str(res.json())


async def test_get_user_group_names(
    client, registered_client, registered_superuser_client
):
    """
    Test the broadly-accessible "GET /auth/group-names/" endpoint.
    """

    # Preliminary phase: create some group(s)
    GROUP_NAME = "my group"
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name=GROUP_NAME)
    )
    assert res.status_code == 201
    debug(res.json())
    EXPECTED_GROUP_NAMES = [GROUP_NAME]
    debug(EXPECTED_GROUP_NAMES)

    # Anonymous user cannot see group names
    res = await client.get(f"{PREFIX}/group-names/")
    assert res.status_code == 401

    # Registered users can see group names
    res = await registered_client.get(f"{PREFIX}/group-names/")
    assert res.status_code == 200
    assert res.json() == EXPECTED_GROUP_NAMES

    # Superusers can see group names
    res = await registered_superuser_client.get(f"{PREFIX}/group-names/")
    assert res.status_code == 200
    assert res.json() == EXPECTED_GROUP_NAMES


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

    # Preliminary phase: create a group and associate a user to it.
    GROUP_NAME = "my group"
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/", json=dict(name=GROUP_NAME)
    )
    assert res.status_code == 201
    group_id = res.json()["id"]

    # Get current user and check it has no group names/ID
    res = await registered_client.get(f"{PREFIX}/current-user/")
    assert res.status_code == 200
    current_user = res.json()
    current_user_id = current_user["id"]
    assert current_user["group_names"] is None
    assert current_user["group_ids"] is None

    # Add current user to group
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_id}/",
        json=dict(new_user_ids=[current_user_id]),
    )
    assert res.status_code == 200

    # Registered user can see group names
    res = await registered_client.get(
        f"{PREFIX}/current-user/?group_names=True"
    )
    assert res.status_code == 200
    current_user = res.json()
    debug(current_user)
    current_user_id = current_user["id"]
    assert current_user["group_names"] == [GROUP_NAME]
    assert current_user["group_ids"] is None

    # Superusers can see group IDs
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/{current_user_id}/"
    )
    assert res.status_code == 200
    user = res.json()
    debug(user)
    assert user["group_names"] is None
    assert user["group_ids"] == [group_id]

    # Superusers don't see group IDs, if group_ids=False
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/{current_user_id}/" "?group_ids=False"
    )
    assert res.status_code == 200
    user = res.json()
    debug(user)
    assert user["group_names"] is None
    assert user["group_ids"] is None
