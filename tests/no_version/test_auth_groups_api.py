from devtools import debug

# import pytest

PREFIX = "/auth"


async def test_no_access_user_group_api(client, registered_client):
    """
    Verify that anonymous or non-superuser users have no access to user-group
    CRUD.
    """
    for _client in [client, registered_client]:
        res = await _client.get(f"{PREFIX}/groups/")
        assert res.status_code == 401

        res = await _client.post(f"{PREFIX}/groups/")
        assert res.status_code == 401

        res = await _client.get(f"{PREFIX}/group/1/")
        assert res.status_code == 401

        res = await _client.patch(f"{PREFIX}/group/1/")
        assert res.status_code == 401

        res = await _client.delete(f"{PREFIX}/group/1/")
        assert res.status_code == 401


async def test_delete_user_group_not_allowed(registered_superuser_client):
    """
    Verify that the user-group DELETE endpoint responds with
    "405 Method Not Allowed".
    """
    res = await registered_superuser_client.delete(f"{PREFIX}/group/1/")
    assert res.status_code == 405


async def test_create_group_invalid_user(registered_superuser_client):
    """
    Creating or modifying a group with an invalid user ID returns a 404.
    """

    # Preliminary: register one new users
    credentials_user_A = dict(email="aaa@example.org", password="12345")
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=credentials_user_A
    )
    assert res.status_code == 201
    user_A_id = res.json()["id"]

    # Preliminary: get all groups (i.e. only the default one)
    res = await registered_superuser_client.get(f"{PREFIX}/groups/")
    assert res.status_code == 200
    groups_data = res.json()
    assert len(groups_data) == 1
    default_group_id = groups_data[0]["id"]

    # Preliminary: check that default group has two users
    group_data = groups_data[0]
    assert len(group_data["user_ids"]) == 2  # FIXME: depends on DB structure

    # Create a new group with a valid and an invalid user
    invalid_user_id = 99999999
    res = await registered_superuser_client.post(
        f"{PREFIX}/groups/",
        json=dict(name="name", user_ids=[user_A_id, invalid_user_id]),
    )
    assert res.status_code == 404

    # Check that group was not created
    res = await registered_superuser_client.get(f"{PREFIX}/groups/")
    assert res.status_code == 200
    groups_data = res.json()
    assert len(groups_data) == 1

    # Patch an existing group by adding an invalid user
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{default_group_id}",
        json=dict(new_user_ids=[invalid_user_id]),
    )
    assert res.status_code == 404


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
        f"{PREFIX}/register/", credentials_user_B
    )
    assert res.status_code == 201
    user_B_id = res.json()["id"]

    # Create group 1 with user A
    res = await registered_superuser_client.post(
        f"{PREFIX}/groups/", json=dict(name="group 1", user_ids=[user_A_id])
    )
    assert res.status_code == 201
    group_1_id = res.json()["id"]
    # Create group 2 with users A and B
    res = await registered_superuser_client.post(
        f"{PREFIX}/groups/",
        json=dict(name="group 2", user_ids=[user_A_id, user_B_id]),
    )
    assert res.status_code == 201
    group_2_id = res.json()["id"]

    # Get group 1
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_1_id}/"
    )
    assert res.status_code == 200
    group_data = res.json()
    assert set(group_data["user_ids"]) == {
        user_A_id
    }  # FIXME: this depends on DB structure

    # Get group 2
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_2_id}/"
    )
    assert res.status_code == 200
    group_data = res.json()
    assert set(group_data["user_ids"]) == {
        user_A_id,
        user_B_id,
    }  # FIXME: this depends on DB structure

    # Get all groups (group 1, group 2 and the default one)
    res = await registered_superuser_client.get(f"{PREFIX}/groups/")
    assert res.status_code == 200
    groups_data = res.json()
    assert len(groups_data) == 3
    # FIXME: add assertions

    # Patch group 1, by adding new member
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_1_id}", json=dict(new_user_ids=[user_B_id])
    )
    # Get group 1
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_1_id}/"
    )
    assert res.status_code == 200
    group_data = res.json()
    assert set(group_data["user_ids"]) == {
        user_A_id,
        user_B_id,
    }  # FIXME: this depends on DB structure

    # Patch group 1, by re-adding existing member
    res = await registered_superuser_client.patch(
        f"{PREFIX}/group/{group_1_id}", json=dict(new_user_ids=[user_B_id])
    )
    # Get group 1
    res = await registered_superuser_client.get(
        f"{PREFIX}/group/{group_1_id}/"
    )
    assert res.status_code == 200
    group_data = res.json()
    assert len(group_data["user_ids"]) == 2
    assert set(group_data["user_ids"]) == {
        user_A_id,
        user_B_id,
    }  # FIXME: this depends on DB structure


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
