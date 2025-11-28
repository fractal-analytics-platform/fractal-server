from devtools import debug

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup

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
