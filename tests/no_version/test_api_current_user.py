import pytest
from devtools import debug

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup

PREFIX = "/auth/current-user/"


async def test_get_current_user(
    client, registered_client, registered_superuser_client
):
    # Anonymous user
    res = await client.get(PREFIX)
    debug(res.json())
    assert res.status_code == 401

    # Registered non-superuser user
    res = await registered_client.get(PREFIX)
    debug(res.json())
    assert res.status_code == 200
    assert not res.json()["is_superuser"]
    assert res.json()["oauth_accounts"] == []

    # Registered superuser
    res = await registered_superuser_client.get(PREFIX)
    debug(res.json())
    assert res.status_code == 200
    assert res.json()["is_superuser"]


async def test_get_current_user_group_ids_names_order(
    client, MockCurrentUser, db, default_user_group
):

    async with MockCurrentUser() as user:
        group1 = UserGroup(name="group1")
        group2 = UserGroup(name="group2")
        db.add(group1)
        db.add(group2)
        await db.commit()
        await db.refresh(group1)
        await db.refresh(group2)
        db.add(LinkUserGroup(user_id=user.id, group_id=group1.id))
        db.add(LinkUserGroup(user_id=user.id, group_id=group2.id))
        await db.commit()

        res = await client.get(f"{PREFIX}?group_ids_names=True")
        assert res.json()["group_ids_names"] == [
            [default_user_group.id, default_user_group.name],
            [group1.id, group1.name],
            [group2.id, group2.name],
        ]

        # Delete and reinsert default group
        link_to_delete = await db.get(
            LinkUserGroup, (default_user_group.id, user.id)
        )
        await db.delete(link_to_delete)
        await db.commit()
        db.add(LinkUserGroup(group_id=default_user_group.id, user_id=user.id))
        await db.commit()

        res = await client.get(f"{PREFIX}?group_ids_names=True")
        assert res.json()["group_ids_names"] == [
            [default_user_group.id, default_user_group.name],
            [group1.id, group1.name],
            [group2.id, group2.name],
        ]

        # Delete and reinsert group1
        link_to_delete = await db.get(LinkUserGroup, (group1.id, user.id))
        await db.delete(link_to_delete)
        await db.commit()
        db.add(LinkUserGroup(group_id=group1.id, user_id=user.id))
        await db.commit()

        res = await client.get(f"{PREFIX}?group_ids_names=True")
        assert res.json()["group_ids_names"] == [
            [default_user_group.id, default_user_group.name],
            [group2.id, group2.name],
            [group1.id, group1.name],
        ]


async def test_patch_current_user_response(registered_client):
    res = await registered_client.get(f"{PREFIX}?group_ids_names=True")
    pre_patch_user = res.json()

    # Successful API call with empty payload
    res = await registered_client.patch(PREFIX, json={})
    assert res.status_code == 200
    assert res.json() == pre_patch_user


async def test_patch_current_user_no_extra(registered_client):
    """
    Test that the PATCH-current-user endpoint fails when extra attributes are
    provided.
    """
    res = await registered_client.patch(PREFIX, json={})
    assert res.status_code == 200
    res = await registered_client.patch(
        PREFIX, json={"cache_dir": "/tmp", "foo": "bar"}
    )
    assert res.status_code == 422


async def test_patch_current_user_password_fails(registered_client, client):
    """
    This test exists for the same reason that test_patch_current_user_password
    is skipped.
    """
    res = await registered_client.patch(PREFIX, json={"password": "something"})
    assert res.status_code == 422


@pytest.mark.skip(reason="Users cannot edit their own password for the moment")
async def test_patch_current_user_password(registered_client, client):
    """
    Test several scenarios for updating `password` for the current user.
    """
    res = await registered_client.get(PREFIX)
    user_email = res.json()["email"]

    # Fail due to null password
    res = await registered_client.patch(PREFIX, json={"password": None})
    assert res.status_code == 422

    # Fail due to empty-string password
    res = await registered_client.patch(PREFIX, json={"password": ""})
    assert res.status_code == 422

    # Fail due to invalid password (too short)
    res = await registered_client.patch(PREFIX, json={"password": "abc"})
    assert res.status_code == 400
    assert "too short" in res.json()["detail"]["reason"]

    # Fail due to invalid password (too long)
    res = await registered_client.patch(PREFIX, json={"password": "x" * 101})
    assert res.status_code == 400
    assert "too long" in res.json()["detail"]["reason"]

    # Successful password update
    NEW_PASSWORD = "my-new-password"
    res = await registered_client.patch(
        PREFIX, json={"password": NEW_PASSWORD}
    )
    assert res.status_code == 200

    # Check that old password is not valid any more
    res = await client.post(
        "auth/token/login/",
        data=dict(
            username=user_email,
            password="12345",  # default password of registered_client
        ),
    )
    assert res.status_code == 400

    # Check that new password is valid
    res = await client.post(
        "auth/token/login/",
        data=dict(
            username=user_email,
            password=NEW_PASSWORD,
        ),
    )
    assert res.status_code == 200


async def test_get_and_patch_current_user_settings(registered_client):
    res = await registered_client.get(f"{PREFIX}settings/")
    assert res.status_code == 200
    for k, v in res.json().items():
        if k == "slurm_accounts":
            assert v == []
        else:
            assert v is None

    patch = dict(slurm_accounts=["foo", "bar"], cache_dir="/tmp/foo_cache")
    res = await registered_client.patch(f"{PREFIX}settings/", json=patch)
    assert res.status_code == 200

    # Assert patch was successful
    res = await registered_client.get(f"{PREFIX}settings/")
    for k, v in res.json().items():
        if k in patch:
            assert v == patch[k]
        else:
            assert v is None


async def test_get_current_user_allowed_viewer_paths(
    registered_client, registered_superuser_client, override_settings_factory
):
    # Start test with "viewer-paths" auth scheme
    override_settings_factory(
        FRACTAL_VIEWER_AUTHORIZATION_SCHEME="viewer-paths"
    )

    # Check that a vanilla user has no viewer_paths
    res = await registered_client.get(f"{PREFIX}allowed-viewer-paths/")
    assert res.status_code == 200
    assert res.json() == []

    # Find current-user ID
    res = await registered_client.get(f"{PREFIX}")
    assert res.status_code == 200
    user_id = res.json()["id"]

    # Add one group to this user
    res = await registered_superuser_client.post(
        "/auth/group/", json=dict(name="group1", viewer_paths=["/a", "/b"])
    )
    assert res.status_code == 201
    group1_id = res.json()["id"]

    # Add user to group1
    res = await registered_superuser_client.post(
        f"/auth/group/{group1_id}/add-user/{user_id}/"
    )
    assert res.status_code == 200

    # Check current-user viewer-paths again
    res = await registered_client.get(f"{PREFIX}allowed-viewer-paths/")
    assert res.status_code == 200
    assert set(res.json()) == {"/a", "/b"}

    # Add one group to this user
    res = await registered_superuser_client.post(
        "/auth/group/", json=dict(name="group2", viewer_paths=["/a", "/c"])
    )
    assert res.status_code == 201
    group2_id = res.json()["id"]

    # Add user to group2
    res = await registered_superuser_client.post(
        f"/auth/group/{group2_id}/add-user/{user_id}/"
    )
    assert res.status_code == 200

    # Update user settings defining project_dir
    res = await registered_superuser_client.patch(
        f"/auth/users/{user_id}/settings/",
        json=dict(project_dir="/path/to/project_dir"),
    )
    assert res.status_code == 200

    # Check that project_dir is used by "viewer-paths" auth scheme
    override_settings_factory(
        FRACTAL_VIEWER_AUTHORIZATION_SCHEME="viewer-paths"
    )
    res = await registered_client.get(f"{PREFIX}allowed-viewer-paths/")
    assert res.status_code == 200
    assert set(res.json()) == {"/path/to/project_dir", "/a", "/b", "/c"}

    # Test with "users-folders" scheme
    override_settings_factory(FRACTAL_VIEWER_BASE_FOLDER="/path/to/base")
    override_settings_factory(
        FRACTAL_VIEWER_AUTHORIZATION_SCHEME="users-folders"
    )
    res = await registered_client.get(f"{PREFIX}allowed-viewer-paths/")
    assert res.status_code == 200
    assert set(res.json()) == {"/path/to/project_dir"}

    # Update user settings adding the slurm_user
    res = await registered_superuser_client.patch(
        f"/auth/users/{user_id}/settings/",
        json=dict(project_dir="/path/to/project_dir", slurm_user="foo"),
    )
    assert res.status_code == 200

    # Test that user dir is added when using "users-folders" scheme
    res = await registered_client.get(f"{PREFIX}allowed-viewer-paths/")
    assert res.status_code == 200
    assert set(res.json()) == {"/path/to/project_dir", "/path/to/base/foo"}

    # Verify that scheme "none" returns an empty list
    override_settings_factory(FRACTAL_VIEWER_AUTHORIZATION_SCHEME="none")
    res = await registered_client.get(f"{PREFIX}allowed-viewer-paths/")
    assert res.status_code == 200
    assert res.json() == []
