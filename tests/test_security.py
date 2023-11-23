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
    payload_register = dict(email=EMAIL, password="12345")

    # Non-superuser user
    res = await registered_client.post(
        f"{PREFIX}/register/", json=payload_register
    )
    debug(res.json())
    assert res.status_code == 403

    # Superuser
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/", json=payload_register
    )
    debug(res.status_code)
    debug(res.json())
    assert res.json()["email"] == EMAIL
    assert res.status_code == 201


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
    res = await registered_client.get(f"{PREFIX}/userlist/")
    assert res.status_code == 403

    # Superuser can list
    res = await registered_superuser_client.get(f"{PREFIX}/userlist/")
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

    # GET/me with non-superuser user
    res = await registered_client.get(f"{PREFIX}/users/me/")
    assert res.status_code == 403

    # GET/{user_id} with superuser user
    res = await registered_superuser_client.get(f"{PREFIX}/users/{user_id}/")
    debug(res.json())
    assert res.status_code == 200


async def test_edit_current_user(registered_client, app):

    res = await registered_client.get(f"{PREFIX}/current-user/")
    expected_status = res.json()

    res = await registered_client.patch(f"{PREFIX}/current-user/", json={})
    assert res.status_code == 200
    assert res.json() == expected_status

    # CACHE_DIR

    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": "/tmp"}
    )
    assert res.status_code == 200
    expected_status["cache_dir"] = "/tmp"
    assert res.json() == expected_status

    # From val_absolute_path:
    # - String attribute 'cache_dir' cannot be empty
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": ""}
    )
    assert res.status_code == 422
    # - String attribute 'cache_dir' must be an absolute path (given 'not_abs')
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": "not_abs"}
    )
    assert res.status_code == 422
    # - String attribute '{attribute}' cannot be None
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"cache_dir": None}
    )
    assert res.status_code == 422

    # PASSWORD

    # FIXME  payload: {"password": null}
    # FIXME  request-body is valid, but the endpoint raises a 422
    #        (through the user manager and fastapi-users)
    # ValueError: "UserOAuth" object has no field "password"
    # Enters SQLModelUserDatabaseAsync.update with {"password":None}
    # (regular password update enters with {"hashed_password":"..."})
    with pytest.raises(ValueError):
        res = await registered_client.patch(
            f"{PREFIX}/current-user/", json={"password": None}
        )

    # Password too short
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": ""}
    )
    assert res.status_code == 400
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": "abcd"}
    )
    assert res.status_code == 400
    # Password too long
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": "x" * 101}
    )
    assert res.status_code == 400
    # Password changed
    NEW_PASSWORD = "x" * 100
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"password": NEW_PASSWORD}
    )
    assert res.status_code == 200

    from httpx import AsyncClient
    from asgi_lifespan import LifespanManager

    async with AsyncClient(
        app=app, base_url="http://test"
    ) as client, LifespanManager(app):
        res = await client.post(
            "auth/token/login/",
            data=dict(
                username=expected_status["email"],
                password="12345",  # default password of registred_clint
            ),
        )
        assert res.status_code == 400  # LOGIN_BAD_CREDENTIALS
        res = await client.post(
            "auth/token/login/",
            data=dict(
                username=expected_status["email"],
                password=NEW_PASSWORD,
            ),
        )
        assert res.status_code == 200

    # NO EXTRA
    res = await registered_client.patch(
        f"{PREFIX}/current-user/", json={"foo": "bar"}
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
    app,
    MockCurrentUser,
    cache_dir,
    username,
    slurm_user,
):

    user_kwargs = dict(
        cache_dir=cache_dir, username=username, slurm_user=slurm_user
    )
    async with MockCurrentUser(persist=False, user_kwargs=user_kwargs) as user:
        debug(user)
        assert user.cache_dir == cache_dir
        assert user.username == username
        assert user.slurm_user == slurm_user
