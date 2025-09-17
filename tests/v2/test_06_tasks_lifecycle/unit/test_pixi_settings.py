import pytest

from fractal_server.config import PixiSLURMConfig


def test_pixi_slurm_config():
    PixiSLURMConfig(
        partition="fake",
        time="100",
        cpus=1,
        mem="10K",
    )
    with pytest.raises(
        ValueError,
        match="units suffix",
    ):
        PixiSLURMConfig(
            partition="fake",
            time="100",
            cpus=1,
            mem="1000",
        )
    PixiSLURMConfig(
        partition="fake",
        time="100",
        cpus=1,
        mem="1000M",
    )
    PixiSLURMConfig(
        partition="fake",
        time="100",
        cpus=1,
        mem="10G",
    )


# def test_pixi_settings():
#     PixiSettings(
#         versions={"1": "/fake/1"},
#         default_version="1",
#         SLURM_CONFIG=
#     )
