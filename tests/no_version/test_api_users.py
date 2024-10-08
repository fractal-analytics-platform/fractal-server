import pytest
from devtools import debug

from fractal_server.app.models.security import OAuthAccount
from fractal_server.app.models.security import UserOAuth

PREFIX = "/auth"


async def test_register_user(registered_client, registered_superuser_client):
    """
    Test that user registration is only allowed to a superuser
    """

    EMAIL = "asd@asd.asd"
    payload_register = dict(email=EMAIL, password="12345")

    # Non-superuser user: FORBIDDEN
    res = await registered_client.post(
        f"{PREFIX}/register/", json=payload_register
    )
    debug(res.json())
    assert res.status_code == 403

    # Superuser: ALLOWED
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=payload_register
    )
    assert res.status_code == 201
    assert res.json()["email"] == EMAIL
    assert res.json()["oauth_accounts"] == []


async def test_list_users(registered_client, registered_superuser_client):
    """
    Test listing users
    """

    # Create two users
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=dict(email="0@asd.asd", password="12345")
    )
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=dict(email="1@asd.asd", password="12345")
    )

    # Non-superuser user is not allowed
    res = await registered_client.get(f"{PREFIX}/users/")
    assert res.status_code == 403

    # Superuser can list
    res = await registered_superuser_client.get(f"{PREFIX}/users/")
    assert res.status_code == 200
    list_emails = [u["email"] for u in res.json()]
    assert "0@asd.asd" in list_emails
    assert "1@asd.asd" in list_emails
    for user in res.json():
        assert user["oauth_accounts"] == []


async def test_show_user(registered_client, registered_superuser_client):

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(email="to_show@asd.asd", password="12345"),
    )
    user_id = res.json()["id"]
    assert res.status_code == 201

    # GET/{user_id} with non-superuser user
    res = await registered_client.get(f"{PREFIX}/users/{user_id}/")
    assert res.status_code == 403

    # GET/{user_id} with superuser user
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/{user_id}/?group_ids=false"
    )
    debug(res.json())
    assert res.status_code == 200
    assert res.json()["oauth_accounts"] == []


async def test_edit_users_as_superuser(registered_superuser_client):

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(email="test@fractal.xy", password="12345"),
    )
    assert res.status_code == 201
    pre_patch_user = res.json()

    # Fail because invalid password
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=dict(password=""),
    )
    assert res.status_code == 400
    debug(res.json())
    assert "The password is too short" in str(res.json()["detail"])

    # succeed
    update = dict(
        email="patch@fractal.xy",
        is_active=False,
        is_superuser=True,
        is_verified=True,
        username="user_patch",
    )
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=update,
    )
    assert res.status_code == 200
    user = res.json()
    debug(user)

    # assert that the attributes we wanted to update have actually changed
    for key, value in user.items():
        if key == "group_ids_names":
            pass
        elif key not in update:
            assert value == pre_patch_user[key]
        else:
            assert value != pre_patch_user[key]
            assert value == update[key]

    user_id = user["id"]

    # EMAIL
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json=dict(email="hello, world!"),
    )
    assert res.status_code == 422

    # Setting the email to an existing one fails with 4
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/",
    )
    assert res.status_code == 200
    users = res.json()
    assert len(users) == 2
    user_0_id = users[0]["id"]
    user_1_email = users[1]["email"]
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_0_id}/",
        json=dict(email=user_1_email),
    )
    assert res.status_code == 400
    assert "UPDATE_USER_EMAIL_ALREADY_EXISTS" == res.json()["detail"]

    for attribute in ["email", "is_active", "is_superuser", "is_verified"]:
        res = await registered_superuser_client.patch(
            f"{PREFIX}/users/{user_id}/",
            json={attribute: None},
        )
        assert res.status_code == 422

    # USERNAME
    # String attribute 'username' cannot be empty
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json={"username": "   "},
    )
    assert res.status_code == 422
    # String attribute 'username' cannot be None
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json={"username": None},
    )
    assert res.status_code == 422


async def test_add_superuser(registered_superuser_client):

    # Create non-superuser user
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(email="future_superuser@asd.asd", password="12345"),
    )
    debug(res.json())
    user_id = res.json()["id"]
    assert res.status_code == 201
    assert not res.json()["is_superuser"]

    # Make user a superuser
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/", json=dict(is_superuser=True)
    )
    debug(res.json())
    assert res.status_code == 200
    assert res.json()["is_superuser"]


async def test_delete_user_method_not_allowed(registered_superuser_client):
    res = await registered_superuser_client.delete(f"{PREFIX}/users/1/")
    assert res.status_code == 405


@pytest.mark.skip(reason="DELETE endpoint is currently disabled")
async def test_delete_user(registered_client, registered_superuser_client):
    """
    Check that DELETE/{user_id} returns some of the correct responses:
        * 204 No content
        * 401 Unauthorized - Missing token or inactive user.
        * 403 Forbidden - Not a superuser.
        * 404 Not found - The user does not exist.
    """

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(email="to_delete@asd.asd", password="1234"),
    )
    user_id = res.json()["id"]
    debug(res.json)
    assert res.status_code == 201

    # Test delete endpoint
    res = await registered_client.delete(f"{PREFIX}/users/{user_id}/")
    assert res.status_code == 403
    res = await registered_superuser_client.delete(
        f"{PREFIX}/users/{user_id}/"
    )
    assert res.status_code == 204
    res = await registered_superuser_client.delete(
        f"{PREFIX}/users/THIS-IS-NOT-AN-ID"
    )
    assert res.status_code == 404


async def test_add_groups_to_user_as_superuser(registered_superuser_client):

    # Create user
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="test@fractal.xy",
            password="12345",
            slurm_accounts=["foo", "bar"],
        ),
    )
    assert res.status_code == 201
    user_id = res.json()["id"]
    res = await registered_superuser_client.get(f"{PREFIX}/users/{user_id}/")
    assert res.status_code == 200
    user = res.json()
    debug(user)
    assert user["group_ids_names"] == []

    # Create group
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/",
        json=dict(name="groupname"),
    )
    assert res.status_code == 201
    group_id = res.json()["id"]

    # Create user/group link and fail because of invalid `group_id``
    invalid_group_id = 999999
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json=dict(new_group_ids=[invalid_group_id]),
    )
    assert res.status_code == 404

    # Create user/group link and succeed
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json=dict(new_group_ids=[group_id]),
    )
    assert res.status_code == 200
    assert res.json()["group_ids_names"] == [[group_id, "groupname"]]

    # Create user/group link and fail because it already exists
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json=dict(new_group_ids=[group_id]),
    )
    assert res.status_code == 422
    hint_msg = "Likely reason: one of these links already exists"
    assert hint_msg in res.json()["detail"]

    # Create user/group link and fail because of repeated IDs
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json=dict(new_group_ids=[99, 99]),
    )
    assert res.status_code == 422
    assert "`new_group_ids` list has repetitions'" in str(res.json())


async def test_edit_user_and_fail(registered_superuser_client):

    # Create user
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="test@fractal.xy",
            password="12345",
        ),
    )
    assert res.status_code == 201
    user_id = res.json()["id"]

    # Patch both user attributes and user/group relationship, and fail
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json=dict(
            username="pippo",
            new_group_ids=[],
        ),
    )
    assert res.status_code == 422
    expected_detail = (
        "Cannot modify both user attributes and group membership."
    )
    assert expected_detail in res.json()["detail"]

    # Make a dummy patch to user, and succeed
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json={},
    )
    assert res.status_code == 200


async def test_oauth_accounts_list(
    client, db, MockCurrentUser, registered_superuser_client
):

    u1 = UserOAuth(email="user1@email.com", hashed_password="abc1")
    u2 = UserOAuth(email="user2@email.com", hashed_password="abc2")
    db.add(u1)
    db.add(u2)
    await db.commit()
    await db.refresh(u1)
    await db.refresh(u2)

    oauth1 = OAuthAccount(
        user_id=u1.id,
        oauth_name="github",
        account_email="user1@github.com",
        account_id="111",
        access_token="aaa",
    )
    oauth2 = OAuthAccount(
        user_id=u1.id,
        oauth_name="google",
        account_email="user1@gmail.com",
        account_id="222",
        access_token="bbb",
    )
    oauth3 = OAuthAccount(
        user_id=u2.id,
        oauth_name="oidc",
        account_email="user2@uzh.com",
        account_id="333",
        access_token="ccc",
    )
    db.add(oauth1)
    db.add(oauth2)
    db.add(oauth3)

    await db.commit()

    # test GET /auth/users/
    res = await registered_superuser_client.get(f"{PREFIX}/users/")
    for user in res.json():
        if user["id"] == u1.id:
            assert len(user["oauth_accounts"]) == 2
        elif user["id"] == u2.id:
            assert len(user["oauth_accounts"]) == 1
        else:
            assert len(user["oauth_accounts"]) == 0

    # test GET /auth/users/{user_id}/
    res = await registered_superuser_client.get(f"{PREFIX}/users/{u1.id}/")
    assert len(res.json()["oauth_accounts"]) == 2
    assert res.json()["group_ids_names"] is not None
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/{u1.id}/?group_ids_names=false"
    )
    assert len(res.json()["oauth_accounts"]) == 2
    assert res.json()["group_ids_names"] is None
    res = await registered_superuser_client.get(f"{PREFIX}/users/{u2.id}/")
    assert len(res.json()["oauth_accounts"]) == 1

    # test PATCH /auth/users/{user_id}
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{u1.id}/", json=dict(password="password")
    )
    assert len(res.json()["oauth_accounts"]) == 2

    # test GET /auth/current-user/
    async with MockCurrentUser(user_kwargs=dict(id=u1.id)):
        res = await client.get(f"{PREFIX}/current-user/")
        assert len(res.json()["oauth_accounts"]) == 2
        res = await client.get(f"{PREFIX}/current-user/?group_names=true")
        assert len(res.json()["oauth_accounts"]) == 2

    # test PATCH /auth/current-user/
    async with MockCurrentUser(user_kwargs=dict(id=u2.id)):
        res = await client.patch(f"{PREFIX}/current-user/", json=dict())
        assert len(res.json()["oauth_accounts"]) == 1


async def test_get_and_patch_user_settings(registered_superuser_client):

    # Register new user
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=dict(email="a@b.c", password="1234")
    )
    assert res.status_code == 201
    user_id = res.json()["id"]

    # Get user settings
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/{user_id}/settings/",
    )
    assert res.status_code == 200
    for k, v in res.json().items():
        if k == "id":
            pass
        elif k == "slurm_accounts":
            assert v == []
        else:
            assert v is None

    # Path user settings
    patch = dict(
        ssh_host="127.0.0.1",
        ssh_username="fractal",
        ssh_private_key_path="/tmp/fractal",
        ssh_tasks_dir="/tmp/tasks",
        # missing "ssh_jobs_dir"
        slurm_user="fractal",
        slurm_accounts=["foo", "bar"],
        cache_dir="/tmp/cache",
    )
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/settings/", json=patch
    )
    debug(res.json())
    assert res.status_code == 200

    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/settings/", json=dict(slurm_accounts=["  "])
    )
    debug(res.json())
    assert res.status_code == 422

    # Assert patch was successful
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/{user_id}/settings/",
    )
    for k, v in res.json().items():
        if k in patch:
            assert v == patch[k]
        elif k == "id":
            pass
        else:
            assert v is None

    # Get non-existing-user settings
    res = await registered_superuser_client.get(f"{PREFIX}/users/42/settings/")
    assert res.status_code == 404
    # Patch non-existing-user settings
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/42/settings/", json=dict()
    )
    assert res.status_code == 404
