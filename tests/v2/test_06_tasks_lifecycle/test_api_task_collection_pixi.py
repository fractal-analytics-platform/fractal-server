from pathlib import Path


async def test_pixi_collection_api_arguments(
    client,
    MockCurrentUser,
    tmp_path: Path,
):
    def empty_tar_gz(filename) -> dict:
        valid_tar_gz = tmp_path / f"{filename}.tar.gz"
        valid_tar_gz.touch()
        with open(valid_tar_gz, "rb") as f:
            tar_gz_content = f.read()
        return {
            "file": (valid_tar_gz.name, tar_gz_content, "application/tar+gzip")
        }

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files={},
        )
        assert res.status_code == 422

        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage-0.1.2-a345"),
        )
        assert res.status_code == 201

        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("my-package-0.1.2-a345"),
        )
        assert res.status_code == 422

        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage-a.b.c"),
        )
        assert res.status_code == 422
