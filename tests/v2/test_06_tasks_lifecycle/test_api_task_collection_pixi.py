from pathlib import Path

from devtools import debug  # noqa

from fractal_server.config import PixiSettings


async def test_pixi_not_available(client, MockCurrentUser):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "9.9.9"},
            files={"file": ("name", b"", "application/tar+gzip")},
        )
        assert res.status_code == 422
        assert res.json()["detail"] == "Pixi task collection is not available."


async def test_api_failures(
    override_settings_factory,
    client,
    MockCurrentUser,
    tmp_path: Path,
):
    override_settings_factory(
        FRACTAL_PIXI_CONFIG_FILE="/fake/pixi/pixi.json",
        pixi=PixiSettings(
            default_version="1.0.0",
            versions={
                "1.0.0": "/fake/pixi/1.0.0",
                "1.0.1": "/fake/pixi/1.0.1",
            },
        ),
    )

    def empty_tar_gz(filename) -> dict:
        valid_tar_gz = tmp_path / f"{filename}.tar.gz"
        valid_tar_gz.touch()
        with open(valid_tar_gz, "rb") as f:
            tar_gz_content = f.read()
        return {
            "file": (valid_tar_gz.name, tar_gz_content, "application/tar+gzip")
        }

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # no data nor files
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={},
            files={},
        )
        assert res.status_code == 422

        # too many hyphens
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage-0.1.2-a345"),
        )
        assert res.status_code == 422

        # no hyphen
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage0.1.2a345"),
        )
        assert res.status_code == 422

        # pixi version not available
        res = await client.post(
            "api/v2/task/collect/pixi/",
            data={"pixi_version": "1.2.3"},
            files=empty_tar_gz("mypackage-0.1.2a345"),
        )
        assert res.status_code == 422
