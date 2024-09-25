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
