from devtools import debug

PREFIX = "/api/v1/task"


async def test_failed_get_collection_info(client, MockCurrentUser):
    """
    Get task-collection info for non-existing collection.
    """
    invalid_state_id = 99999
    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/collect/{invalid_state_id}/")
    debug(res)
    assert res.status_code == 404


async def test_collection_non_verified_user(client, MockCurrentUser):
    """
    Test that non-verified users are not authorized to make calls
    to `/api/v1/task/collect/pip/`.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(
            f"{PREFIX}/collect/pip/", json={"package": "fractal-tasks-core"}
        )
        assert res.status_code == 401
