from devtools import debug

from fractal_server.app.schemas.v2 import PixiVersionCreate


async def test_pixi(client, MockCurrentUser):

    async with MockCurrentUser():

        # Get single empty
        res = await client.get("api/v2/pixi/")
        assert res.status_code == 200
        assert res.json() == []

        pixi1 = PixiVersionCreate(version="1.2.3", path="/pixi-bin/pixi123")
        pixi2 = PixiVersionCreate(version="3.1.4", path="/pixi-bin/pixi314")
        pixi3 = PixiVersionCreate(version="0.0.1", path="/some/path/pixi001")

        # Post
        for pixi in [pixi1, pixi2, pixi3]:
            res = await client.post("api/v2/pixi/", json=pixi.model_dump())
            assert res.status_code == 201
            assert res.json()["version"] == pixi.version
            assert res.json()["path"] == pixi.path

        # Post fail
        for repeated_version in [
            PixiVersionCreate(version="99.99.99", path=pixi1.path),
            PixiVersionCreate(version=pixi2.version, path="/new_path"),
            pixi3,
        ]:
            res = await client.post(
                "api/v2/pixi/", json=repeated_version.model_dump()
            )
            assert res.status_code == 422
            debug(res.json())

        # Get list
        res = await client.get("api/v2/pixi/")
        assert res.status_code == 200
        assert len(res.json()) == 3
        assert [pixi["version"] for pixi in res.json()] == [
            pixi2.version,
            pixi1.version,
            pixi3.version,
        ]

        # Get single
        res = await client.get(f"api/v2/pixi/{pixi1.version}/")
        assert res.status_code == 200
        assert res.json()["version"] == pixi1.version
        assert res.json()["path"] == pixi1.path

        # Get single fail
        res = await client.get("api/v2/pixi/30.11.93/")
        assert res.status_code == 404
