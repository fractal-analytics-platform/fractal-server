import pytest
from devtools import debug


PREFIX = "/auth"


async def test_whoami(client, MockCurrentUser):

    # Anonymous user
    res = await client.get(f"{PREFIX}/whoami/")
    debug(res.json())
    assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        # Registered non-superuser user
        res = await client.get(f"{PREFIX}/whoami/")
        debug(res.json())
        assert res.status_code == 200
        assert not res.json()["is_superuser"]

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        # Registered superuser
        res = await client.get(f"{PREFIX}/whoami/")
        debug(res.json())
        assert res.status_code == 200
        assert res.json()["is_superuser"]


async def test_register_user(client, MockCurrentUser):
    """
    Test that user registration is only allowed to a superuser
    """

    payload_register = dict(email="asd@asd.asd", password="1234")

    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        # Non-superuser user
        res = await client.post(f"{PREFIX}/register/", json=payload_register)
        debug(res.json())
        assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        # Superuser
        res = await client.post(f"{PREFIX}/register/", json=payload_register)
        debug(res.status_code)
        debug(res.json())
        assert res.json()["email"] == payload_register["email"]
        assert res.status_code == 201


async def test_list_users(client, MockCurrentUser):
    """
    Test listing users
    """

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        # Create two users
        res = await client.post(
            f"{PREFIX}/register/", json=dict(email="0@asd.asd", password="12")
        )
        res = await client.post(
            f"{PREFIX}/register/", json=dict(email="1@asd.asd", password="12")
        )

    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as sudo:
        # Non-superuser user is not allowed
        res = await client.get(f"{PREFIX}/userlist/")
        debug(res, sudo)
        assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        # Superuser can list
        res = await client.get(f"{PREFIX}/userlist/")
        debug(res.json())
        list_emails = [u["email"] for u in res.json()]
        assert "0@asd.asd" in list_emails
        assert "1@asd.asd" in list_emails
        assert res.status_code == 200


async def test_show_user(registered_client, registered_superuser_client):

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(email="to_show@asd.asd", password="12"),
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


async def test_edit_user(registered_client, registered_superuser_client):

    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(email="to_patch@asd.asd", password="12"),
    )
    user_id = res.json()["id"]
    assert res.status_code == 201

    # PATCH/{user_id} with non-superuser user
    res = await registered_client.patch(
        f"{PREFIX}/users/{user_id}/", json={"slurm_user": "my_slurm_user"}
    )
    assert res.status_code == 403

    # PATCH/me with non-superuser user
    res = await registered_client.patch(
        f"{PREFIX}/users/me/", json={"slurm_user": "asd"}
    )
    assert res.status_code == 403

    # PATCH/{user_id} with superuser
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/",
        json={
            "slurm_user": "my_slurm_user",
            "cache_dir": "/some/absolute/path",
        },
    )
    assert res.status_code == 200
    assert res.json()["slurm_user"] == "my_slurm_user"
    assert res.json()["cache_dir"] == "/some/absolute/path"

    # PATCH/{user_id} with superuser, but with invalid payload
    res = await registered_superuser_client.patch(
        f"{PREFIX}/users/{user_id}/", json={"cache_dir": "non/absolute/path"}
    )
    debug(res.json())
    assert res.status_code == 422


async def test_add_superuser(registered_superuser_client):

    # Create non-superuser user
    res = await registered_superuser_client.post(
        f"{PREFIX}/register/",
        json=dict(email="future_superuser@asd.asd", password="12"),
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
        json=dict(email="to_delete@asd.asd", password="12"),
    )
    user_id = res.json()["id"]
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
    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        debug(user)
        assert user.cache_dir == cache_dir
        assert user.username == username
        assert user.slurm_user == slurm_user
