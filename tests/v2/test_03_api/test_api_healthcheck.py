async def test_healthcheck(client, db, MockCurrentUser, tmp_path):

    async with MockCurrentUser():
        res = await client.post(
            "/api/v2/healthcheck/", json={"zarr_dir": tmp_path.as_posix()}
        )
        assert res.status_code == 200
