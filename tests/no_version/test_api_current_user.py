from devtools import debug

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.config import DataAuthScheme
from tests.fixtures_server import PROJECT_DIR_PLACEHOLDER

PREFIX = "/auth/current-user/"


async def test_get_current_user(client, MockCurrentUser):
    # Anonymous user
    res = await client.get(PREFIX)
    debug(res.json())
    assert res.status_code == 401

    # Registered non-superuser user
    async with MockCurrentUser():
        res = await client.get(PREFIX)
        debug(res.json())
        assert res.status_code == 200
        assert not res.json()["is_superuser"]
        assert res.json()["oauth_accounts"] == []

    # Registered superuser
    async with MockCurrentUser(user_kwargs=dict(is_superuser=True)):
        res = await client.get(PREFIX)
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


async def test_patch_current_user_response(client, MockCurrentUser):
    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}?group_ids_names=True")
        pre_patch_user = res.json()

        # Successful API call with empty payload
        res = await client.patch(PREFIX, json={})
        assert res.status_code == 200
        assert res.json() == pre_patch_user


async def test_patch_current_user_password_fails(MockCurrentUser, client):
    """
    Users cannot edit their own password.
    """
    async with MockCurrentUser():
        res = await client.patch(PREFIX, json={"password": "something"})
        assert res.status_code == 422


async def test_get_current_user_allowed_viewer_paths(
    MockCurrentUser,
    client,
    override_data_settings_factory,
    slurm_sudo_resource_profile_db,
):
    # Start test with "viewer-paths" auth scheme
    override_data_settings_factory(
        FRACTAL_DATA_AUTH_SCHEME=DataAuthScheme.VIEWER_PATHS
    )

    # Check that a vanilla user has no viewer_paths
    async with MockCurrentUser() as user:
        user_id = user.id

        res = await client.get(f"{PREFIX}allowed-viewer-paths/")
        assert res.status_code == 200
        assert set(res.json()) == {user.project_dir}

        # Add one group to this user
        async with MockCurrentUser(
            user_kwargs=dict(is_superuser=True)
        ) as superuser:
            superuser_id = superuser.id
            res = await client.post(
                "/auth/group/",
                json=dict(name="group1", viewer_paths=["/a", "/b"]),
            )
            assert res.status_code == 201
            group1_id = res.json()["id"]

        # Add user to group1
        res = await client.post(f"/auth/group/{group1_id}/add-user/{user_id}/")
        assert res.status_code == 200

        # Check current-user viewer-paths again
        res = await client.get(f"{PREFIX}allowed-viewer-paths/")
        assert res.status_code == 200
        assert set(res.json()) == {"/a", "/b", PROJECT_DIR_PLACEHOLDER}

    # Add one group to this user
    async with MockCurrentUser(user_kwargs=dict(id=superuser_id)):
        res = await client.post(
            "/auth/group/", json=dict(name="group2", viewer_paths=["/a", "/c"])
        )
        assert res.status_code == 201
        group2_id = res.json()["id"]

        # Add user to group2
        res = await client.post(f"/auth/group/{group2_id}/add-user/{user_id}/")
        assert res.status_code == 200

        # Update user, defining project_dir
        res = await client.patch(
            f"/auth/users/{user_id}/",
            json=dict(project_dir=PROJECT_DIR_PLACEHOLDER),
        )
        assert res.status_code == 200

    # Check that project_dir is used by "viewer-paths" auth scheme
    async with MockCurrentUser(user_kwargs=dict(id=user_id)):
        res = await client.get(f"{PREFIX}allowed-viewer-paths/")
        assert res.status_code == 200
        assert set(res.json()) == {PROJECT_DIR_PLACEHOLDER, "/a", "/b", "/c"}

    # Test with "users-folders" scheme
    override_data_settings_factory(
        FRACTAL_DATA_AUTH_SCHEME=DataAuthScheme.USERS_FOLDERS,
        FRACTAL_DATA_BASE_FOLDER="/path/to/base",
    )
    async with MockCurrentUser(user_kwargs=dict(profile_id=None)):
        res = await client.get(f"{PREFIX}allowed-viewer-paths/")
        assert res.status_code == 200
        assert set(res.json()) == {PROJECT_DIR_PLACEHOLDER}

    # Test that user dir is added when using "users-folders" scheme
    resource, profile = slurm_sudo_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            id=user_id,
            profile_id=profile.id,
        )
    ):
        res = await client.get(f"{PREFIX}allowed-viewer-paths/")
        assert res.status_code == 200
        assert set(res.json()) == {
            PROJECT_DIR_PLACEHOLDER,
            f"/path/to/base/{profile.username}",
        }

    # Verify that scheme "none" returns an empty list
    override_data_settings_factory(
        FRACTAL_DATA_AUTH_SCHEME=DataAuthScheme.NONE
    )
    async with MockCurrentUser(user_kwargs=dict(id=user_id)):
        res = await client.get(f"{PREFIX}allowed-viewer-paths/")
        assert res.status_code == 200
        assert res.json() == []
