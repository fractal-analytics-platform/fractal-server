import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy.orm import Session as DBSyncSession

from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.tasks.v2.background_operations import _insert_tasks
from fractal_server.tasks.v2.background_operations import (
    _prepare_tasks_metadata,
)


def run_cmd(cmd: str):
    res = subprocess.run(  # nosec
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if res.returncode != 0:
        raise subprocess.CalledProcessError(
            res.returncode, cmd=cmd, output=res.stdout, stderr=res.stderr
        )
    return res.stdout


@pytest.fixture(scope="session")
def fractal_tasks_mock_venv_new(tmpdir_factory, testdata_path) -> Path:

    VENV_NAME = "venv"
    base_dir = Path(tmpdir_factory.getbasetemp())
    venv_dir = base_dir / VENV_NAME
    venv_python = venv_dir / "bin/python"
    whl = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()

    if not venv_dir.exists():
        run_cmd(f"{sys.executable} -m venv {venv_dir}")
        run_cmd(f"{venv_python} -m pip install {whl}")

    return venv_python


@pytest.fixture
def fractal_tasks_mock_collection_new(
    fractal_tasks_mock_venv_new: Path, db_sync: DBSyncSession
) -> list[TaskV2]:

    package_name = "fractal_tasks_mock"

    python_command = (
        "import importlib.util; "
        "from pathlib import Path; "
        "init_path=importlib.util.find_spec"
        f'("{package_name}").origin; '
        "print(Path(init_path).parent.as_posix())"
    )

    res = run_cmd(f"{fractal_tasks_mock_venv_new} -c '{python_command}'")
    package_root = Path(res.strip("\n"))

    with open(package_root / "__FRACTAL_MANIFEST__.json", "r") as f:
        manifest_dict = json.load(f)
    manifest = ManifestV2(**manifest_dict)

    task_list: list[TaskCreateV2] = _prepare_tasks_metadata(
        package_manifest=manifest,
        package_source="pytest",
        python_bin=fractal_tasks_mock_venv_new,
        package_root=package_root,
    )

    task_list_db: list[TaskV2] = _insert_tasks(task_list, db_sync)

    return task_list_db
