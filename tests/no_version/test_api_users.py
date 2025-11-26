from devtools import debug

from fractal_server.app.models.security import OAuthAccount
from tests.fixtures_server import PROJECT_DIR_PLACEHOLDER

PREFIX = "/auth"


async def test_register_user(
    registered_client, registered_superuser_client, local_resource_profile_db
):
    """
    Test that user registration is only allowed to a superuser
    """

    EMAIL = "asd@asd.asd"
    payload_register = dict(
        email=EMAIL,
        password="12345",
        project_dirs=[PROJECT_DIR_PLACEHOLDER],
    )

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
    assert res.json()["profile_id"] is None

    # Superuser: ALLOWED
    EMAIL = "asd2@asd.asd"
    payload_register2 = dict(
        email=EMAIL,
        password="12345",
        project_dirs=[PROJECT_DIR_PLACEHOLDER],
    )

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=payload_register2
    )
    debug(res.json())
    assert res.status_code == 201
    assert res.json()["email"] == EMAIL
    assert res.json()["oauth_accounts"] == []
    assert res.json()["profile_id"] is None

    _, profile = local_resource_profile_db
    EMAIL = "asd3@asd.asd"
    payload_register3 = dict(
        email=EMAIL,
        password="12345",
        profile_id=profile.id,
        project_dirs=[PROJECT_DIR_PLACEHOLDER],
    )
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=payload_register3
    )
    assert res.status_code == 201
    assert res.json()["email"] == EMAIL
    assert res.json()["oauth_accounts"] == []
    assert res.json()["profile_id"] == profile.id


async def test_list_users(registered_client, registered_superuser_client):
    """
    Test listing users
    """

    # Create two users
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="0@asd.asd",
            password="12345",
            project_dirs=[PROJECT_DIR_PLACEHOLDER],
        ),
    )
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="1@asd.asd",
            password="12345",
            project_dirs=[PROJECT_DIR_PLACEHOLDER],
        ),
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
        json=dict(
            email="to_show@asd.asd",
            password="12345",
            project_dirs=[PROJECT_DIR_PLACEHOLDER],
        ),
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


async def test_edit_users_as_superuser(
    registered_superuser_client, local_resource_profile_db
):
    _, profile = local_resource_profile_db

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="test@example.org",
            password="12345",
            project_dirs=[PROJECT_DIR_PLACEHOLDER],
        ),
    )
    assert res.status_code == 201
    pre_patch_user = res.json()

    assert pre_patch_user["profile_id"] is None

    # Fail because invalid password
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=dict(password=""),
    )
    assert res.status_code == 422

    # Fail because `project_dirs=None`
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=dict(project_dirs=None),
    )
    assert res.status_code == 422

    # Fail because `slurm_accounts=None`
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=dict(slurm_accounts=None),
    )
    assert res.status_code == 422

    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=dict(password="abc"),
    )
    assert res.status_code == 400
    debug(res.json())
    assert "The password is too short" in str(res.json()["detail"])

    # Fail because invalid profile_id
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=dict(profile_id=9999),
    )
    assert res.status_code == 404

    # succeed
    update = dict(
        email="patch@example.org",
        is_active=False,
        is_superuser=True,
        is_verified=True,
        profile_id=profile.id,
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

    # Set `profile_id` query parameter
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/?profile_id=987654321",
    )
    assert res.status_code == 200
    users = res.json()
    assert len(users) == 0


async def test_add_superuser(registered_superuser_client):
    # Create non-superuser user
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="future_superuser@asd.asd",
            password="12345",
            project_dirs=[PROJECT_DIR_PLACEHOLDER],
        ),
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


async def test_set_groups_endpoint(
    registered_superuser_client,
    default_user_group,
):
    # Preliminary step: create a user
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="test@example.org",
            password="12345",
            project_dirs=[PROJECT_DIR_PLACEHOLDER],
            slurm_accounts=["foo", "bar"],
        ),
    )
    assert res.status_code == 201
    user_id = res.json()["id"]
    res = await registered_superuser_client.get(f"{PREFIX}/users/{user_id}/")
    assert res.status_code == 200
    user = res.json()
    assert user["group_ids_names"] == [
        [default_user_group.id, default_user_group.name]
    ]

    # Preliminary step: create a user group
    GROUP_NAME = "my group"
    res = await registered_superuser_client.post(
        f"{PREFIX}/group/",
        json=dict(name=GROUP_NAME),
    )
    assert res.status_code == 201
    group_id = res.json()["id"]

    # Test `/users/{user_id}/set-groups/`

    # Failure: Empty request body
    res = await registered_superuser_client.post(
        f"{PREFIX}/users/{user_id}/set-groups/",
        json=dict(group_ids=[]),
    )
    assert res.status_code == 422
    MSG = "List should have at least 1 item after validation, not 0"
    assert MSG in str(res.json()["detail"])

    # Failure: Repeated request-body values
    res = await registered_superuser_client.post(
        f"{PREFIX}/users/{user_id}/set-groups/",
        json=dict(group_ids=[99, 99]),
    )
    assert res.status_code == 422
    assert "has repetitions" in str(res.json()["detail"])

    # Failure: Invalid group_id
    invalid_group_id = 999999
    res = await registered_superuser_client.post(
        f"{PREFIX}/users/{user_id}/set-groups/",
        json=dict(group_ids=[invalid_group_id]),
    )
    assert res.status_code == 404

    # Failure: Invalid user_id
    invalid_user_id = 999999
    res = await registered_superuser_client.post(
        f"{PREFIX}/users/{invalid_user_id}/set-groups/",
        json=dict(group_ids=[default_user_group.id]),
    )
    assert res.status_code == 404

    # Failure: you cannot remove the link to `All`
    res = await registered_superuser_client.post(
        f"{PREFIX}/users/{user_id}/set-groups/",
        json=dict(group_ids=[group_id]),
    )
    assert res.status_code == 422
    MSG = "Cannot remove user from 'All' group"
    assert MSG in str(res.json()["detail"])

    # Success
    res = await registered_superuser_client.post(
        f"{PREFIX}/users/{user_id}/set-groups/",
        json=dict(group_ids=[group_id, default_user_group.id]),
    )
    assert res.status_code == 200
    assert res.json()["group_ids_names"] == [
        [default_user_group.id, default_user_group.name],
        [group_id, GROUP_NAME],
    ]

    # Success
    res = await registered_superuser_client.post(
        f"{PREFIX}/users/{user_id}/set-groups/",
        json=dict(group_ids=[default_user_group.id]),
    )
    assert res.status_code == 200
    assert res.json()["group_ids_names"] == [
        [default_user_group.id, default_user_group.name],
    ]


async def test_oauth_accounts_list(
    client, db, MockCurrentUser, registered_superuser_client
):
    async with MockCurrentUser(user_kwargs=dict(email="user1@email.org")) as u1:
        u1_id = u1.id
    async with MockCurrentUser(user_kwargs=dict(email="user2@email.org")) as u2:
        u2_id = u2.id

    oauth1 = OAuthAccount(
        user_id=u1.id,
        oauth_name="github",
        account_email="user1@github.com",
        account_id="111",
        access_token="aaa",
    )
    oauth2 = OAuthAccount(
        user_id=u1_id,
        oauth_name="google",
        account_email="user1@gmail.com",
        account_id="222",
        access_token="bbb",
    )
    oauth3 = OAuthAccount(
        user_id=u2_id,
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
        if user["id"] == u1_id:
            assert len(user["oauth_accounts"]) == 2
        elif user["id"] == u2_id:
            assert len(user["oauth_accounts"]) == 1
        else:
            assert len(user["oauth_accounts"]) == 0

    # test GET /auth/users/{user_id}/
    res = await registered_superuser_client.get(f"{PREFIX}/users/{u1_id}/")
    assert len(res.json()["oauth_accounts"]) == 2
    assert res.json()["group_ids_names"] is not None
    res = await registered_superuser_client.get(
        f"{PREFIX}/users/{u1.id}/?group_ids_names=false"
    )
    assert len(res.json()["oauth_accounts"]) == 2
    assert res.json()["group_ids_names"] is None
    res = await registered_superuser_client.get(f"{PREFIX}/users/{u2_id}/")
    assert len(res.json()["oauth_accounts"]) == 1

    # test PATCH /auth/users/{user_id}/
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{u1_id}/", json=dict(password="password")
    )
    assert len(res.json()["oauth_accounts"]) == 2

    # test GET /auth/current-user/
    async with MockCurrentUser(user_kwargs=dict(id=u1_id)):
        res = await client.get(f"{PREFIX}/current-user/")
        assert len(res.json()["oauth_accounts"]) == 2
        res = await client.get(f"{PREFIX}/current-user/?group_names=true")
        assert len(res.json()["oauth_accounts"]) == 2

    # test PATCH /auth/current-user/
    async with MockCurrentUser(user_kwargs=dict(id=u2_id)):
        res = await client.patch(f"{PREFIX}/current-user/", json=dict())
        assert len(res.json()["oauth_accounts"]) == 1


async def test_get_profile_info(
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db

    async with MockCurrentUser(user_kwargs=dict(profile_id=None)):
        res = await client.get("/auth/current-user/profile-info/")
        assert res.status_code == 200
        assert res.json() == {
            "has_profile": False,
            "resource_name": None,
            "profile_name": None,
            "username": None,
        }

    async with MockCurrentUser(user_kwargs=dict(profile_id=profile.id)):
        res = await client.get("/auth/current-user/profile-info/")
        assert res.status_code == 200
        assert res.json() == {
            "has_profile": True,
            "resource_name": resource.name,
            "profile_name": profile.name,
            "username": profile.username,
        }
