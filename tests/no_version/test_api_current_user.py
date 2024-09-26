import pytest
from devtools import debug

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


async def test_patch_current_user_response(registered_client):
    res = await registered_client.get(f"{PREFIX}?group_names=True")
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
