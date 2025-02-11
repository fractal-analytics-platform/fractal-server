from sqlmodel import select

from fractal_server.app.models import JobV2


async def test_healthcheck(client, db, MockCurrentUser, tmp_path):

    res = await db.execute(select(JobV2))
    assert len(res.scalars().all()) == 0

    async with MockCurrentUser():
        res = await client.post(
            "/api/v2/healthcheck/", json={"zarr_dir": tmp_path.as_posix()}
        )
        assert res.status_code == 200
        res = await client.post(
            "/api/v2/healthcheck/", json={"zarr_dir": tmp_path.as_posix()}
        )
        assert res.status_code == 200

    res = await db.execute(select(JobV2))
    assert len(res.scalars().all()) == 2
