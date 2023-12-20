import pytest
from devtools import debug

PREFIX = "/auth"


async def test_get_current_user(
    client, registered_client, registered_superuser_client
):

    # Anonymous user
    res = await client.get(f"{PREFIX}/current-user/")
    debug(res.json())
    assert res.status_code == 401

    # Registered non-superuser user
    res = await registered_client.get(f"{PREFIX}/current-user/")
    debug(res.json())
    assert res.status_code == 200
    assert not res.json()["is_superuser"]

    # Registered superuser
    res = await registered_superuser_client.get(f"{PREFIX}/current-user/")
    debug(res.json())
    assert res.status_code == 200
    assert res.json()["is_superuser"]


async def test_register_user(registered_client, registered_superuser_client):
    """
    Test that user registration is only allowed to a superuser
    """

    EMAIL = "asd@asd.asd"
    payload_register = dict(
        email=EMAIL, password="12345", slurm_accounts=["A", "B"]
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
    assert res.json()["slurm_accounts"] == payload_register["slurm_accounts"]


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
    debug(res.json())
    list_emails = [u["email"] for u in res.json()]
    assert "0@asd.asd" in list_emails
    assert "1@asd.asd" in list_emails
    assert res.status_code == 200


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
    res = await registered_superuser_client.get(f"{PREFIX}/users/{user_id}/")
    debug(res.json())
    assert res.status_code == 200


async def test_patch_current_user_cache_dir(registered_client):
    """
    Test several scenarios for updating `slurm_accounts` and `cache_dir`
    for the current user.
    """
    res = await registered_client.get(f"{PREFIX}/current-user/")
    pre_patch_user = res.json()

    # Successful API call with empty payload
    res = await registered_client.patch(f"{PREFIX}/current-user/", json={})
    assert res.status_code == 200
    assert res.json() == pre_patch_user

    # Successful update
    assert pre_patch_user["cache_dir"] is None
    NEW_SLURM_ACCOUNTS = ["foo", "bar"]
    assert pre_patch_user["slurm_accounts"] != NEW_SLURM_ACCOUNTS
    res = await registered_client.patch(
        f"{PREFIX}/current-user/",
        json={"cache_dir": "/tmp", "slurm_accounts": NEW_SLURM_ACCOUNTS},
    )
    assert res.status_code == 200
    assert res.json()["cache_dir"] == "/tmp"
    assert res.json()["slurm_accounts"] == NEW_SLURM_ACCOUNTS

    # slurm_accounts must be a list of StrictStr without repetitions
    res = await registered_client.patch(
        f"{PREFIX}/current-user/",
        json={"slurm_accounts": ["a", "b", "c"]},
    )
    assert res.status_code == 200
    assert res.json()["slurm_accounts"] == ["a", "b", "c"]

    # Failed update due to empty string
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": ""}
    )
    assert res.status_code == 422

    # Failed update due to null value
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": None}
    )
    assert res.status_code == 422

    # Failed update due to non-absolute path
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": "not_abs"}
    )
    assert res.status_code == 422


async def test_patch_current_user_no_extra(registered_client):
    """
    Test that the PATCH-current-user endpoint fails when extra attributes are
    provided.
    """
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": "/tmp", "foo": "bar"}
    )
    assert res.status_code == 422


async def test_patch_current_user_password_fails(registered_client, client):
    """
    This test exists for the same reason that test_patch_current_user_password
    is skipped.
    """
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": "something"}
    )
    assert res.status_code == 422


@pytest.mark.skip(reason="Users cannot edit their own password for the moment")
async def test_patch_current_user_password(registered_client, client):
    """
    Test several scenarios for updating `password` for the current user.
    """
    res = await registered_client.get(f"{PREFIX}/current-user/")
    user_email = res.json()["email"]

    # Fail due to null password
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": None}
    )
    assert res.status_code == 422

    # Fail due to empty-string password
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": ""}
    )
    assert res.status_code == 422

    # Fail due to invalid password (too short)
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": "abc"}
    )
    assert res.status_code == 400
    assert "too short" in res.json()["detail"]["reason"]

    # Fail due to invalid password (too long)
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": "x" * 101}
    )
    assert res.status_code == 400
    assert "too long" in res.json()["detail"]["reason"]

    # Successful password update
    NEW_PASSWORD = "my-new-password"
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": NEW_PASSWORD}
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


async def test_edit_users_as_superuser(registered_superuser_client):

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(
            email="test@fractal.xy",
            password="12345",
            slurm_accounts=["foo", "bar"],
        ),
    )
    assert res.status_code == 201
    pre_patch_user = res.json()

    update = dict(
        email="patch@fractal.xy",
        is_active=False,
        is_superuser=True,
        is_verified=True,
        slurm_user="slurm_patch",
        cache_dir="/patch",
        username="user_patch",
        slurm_accounts=["FOO", "BAR", "FOO"],
    )
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=update,
    )
    # Fail because of repeated "FOO" in update.slurm_accounts
    assert res.status_code == 422
    # remove one of the two "FOO" in update.slurm_accounts
    update["slurm_accounts"] = ["FOO", "BAR"]
    # succeed without the repetition
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{pre_patch_user['id']}/",
        json=update,
    )
    assert res.status_code == 200
    user = res.json()
    # assert that the attributes we wanted to update have actually changed
    for key, value in user.items():
        if key not in update:
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

    for attribute in ["email", "is_active", "is_superuser", "is_verified"]:
        res = await registered_superuser_client.patch(
            f"{PREFIX}/users/{user_id}/",
            json={attribute: None},
        )
        assert res.status_code == 422

    # SLURM_USER
    # String attribute 'slurm_user' cannot be empty
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json={"slurm_user": "      "},
    )
    assert res.status_code == 422
    # String attribute 'slurm_user' cannot be None
    assert res.status_code == 422
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json={"slurm_user": None},
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

    # CACHE_DIR
    # String attribute 'cache_dir' cannot be None
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/", json={"cache_dir": None}
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


@pytest.mark.parametrize("cache_dir", ("/some/path", None))
@pytest.mark.parametrize("username", ("my_username", None))
@pytest.mark.parametrize("slurm_user", ("test01", None))
async def test_MockCurrentUser_fixture(
    db,
    MockCurrentUser,
    cache_dir,
    username,
    slurm_user,
):

    user_kwargs = dict(
        cache_dir=cache_dir, username=username, slurm_user=slurm_user
    )
    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        debug(user)
        assert user.cache_dir == cache_dir
        assert user.username == username
        assert user.slurm_user == slurm_user
