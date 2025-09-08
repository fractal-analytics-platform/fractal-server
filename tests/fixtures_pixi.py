import logging
import shlex
import subprocess
from pathlib import Path

import pytest
from pytest import TempdirFactory

from fractal_server.config import PixiSettings


def run_cmd(cmd: str):
    res = subprocess.run(  # nosec
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if res.returncode != 0:
        raise subprocess.CalledProcessError(
            res.returncode,
            cmd=cmd,
            output=res.stdout,
            stderr=res.stderr,
        )
    return res.stdout


@pytest.fixture(scope="session")
def pixi_pkg_targz(testdata_path: Path) -> Path:
    return testdata_path / "mock_pixi_tasks-0.2.1.tar.gz"


@pytest.fixture(scope="session")
def pixi(
    tmpdir_factory: TempdirFactory,
) -> PixiSettings:
    """
    Session scoped fixture that installs pixi 0.54.1.
    """
    base_dir = Path(tmpdir_factory.getbasetemp())
    pixi_common = base_dir / "pixi"
    pixi_common.mkdir()

    pixi_home = (pixi_common / "0.54.1").as_posix()
    script_contents = (
        "export PIXI_NO_PATH_UPDATE=1\n"
        "export PIXI_VERSION=0.54.1\n"
        f"export PIXI_HOME={pixi_home}\n"
        "curl -fsSL https://pixi.sh/install.sh | sh\n"
    )
    print(script_contents)
    script_path = pixi_common / "install_pixi.sh"
    with script_path.open("w") as f:
        f.write(script_contents)
    cmd = f"bash {script_path.as_posix()}"
    logging.info(f"START running {cmd=}")
    run_cmd(cmd)
    logging.info(f"END   running {cmd=}")

    return PixiSettings(
        default_version="0.54.1",
        versions={"0.54.1": pixi_home},
    )
