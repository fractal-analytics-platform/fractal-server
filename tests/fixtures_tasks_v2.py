import json
import shlex
import subprocess
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
def fractal_tasks_mock_venv(tmpdir_factory, testdata_path) -> Path:

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
        run_cmd(f"python3.9 -m venv {venv_dir}")
        run_cmd(f"{venv_python} -m pip install {whl}")

    return venv_python


@pytest.fixture
def fractal_tasks_mock_collection(
    fractal_tasks_mock_venv: Path, db_sync: DBSyncSession
) -> dict[str, dict]:

    package_name = "fractal_tasks_mock"

    python_command = (
        "import importlib.util; "
        "from pathlib import Path; "
        "init_path=importlib.util.find_spec"
        f'("{package_name}").origin; '
        "print(Path(init_path).parent.as_posix())"
    )

    res = run_cmd(f"{fractal_tasks_mock_venv} -c '{python_command}'")
    package_root = Path(res.strip("\n"))

    with open(package_root / "__FRACTAL_MANIFEST__.json", "r") as f:
        manifest_dict = json.load(f)
    manifest = ManifestV2(**manifest_dict)

    task_list: list[TaskCreateV2] = _prepare_tasks_metadata(
        package_manifest=manifest,
        package_source="pytest",
        python_bin=fractal_tasks_mock_venv,
        package_root=package_root,
    )

    task_list_db: list[TaskV2] = _insert_tasks(task_list, db_sync)

    task_dict = {}
    for ind, task in enumerate(task_list_db):
        task_attributes = dict(
            id=ind,
            name=task.name,
            source=task.name.replace(" ", "_"),
        )
        if task.name == "MIP_compound":
            task_attributes.update(
                dict(
                    input_types={"3D": True},
                    output_types={"3D": False},
                )
            )
        elif task.name in [
            "illumination_correction",
            "illumination_correction_compound",
        ]:
            task_attributes.update(
                dict(
                    input_types={"illumination_correction": False},
                    output_types={"illumination_correction": True},
                )
            )
        elif task.name == "apply_registration_to_image":
            task_attributes.update(
                dict(
                    input_types={"registration": False},
                    output_types={"registration": True},
                )
            )
        elif task.name == "generic_task_parallel":
            task_attributes.update(
                dict(
                    input_types={"my_type": False},
                    output_types={"my_type": True},
                )
            )
        for step in ["non_parallel", "parallel"]:
            key = f"command_{step}"
            if task.model_dump().get(key) is not None:
                task_attributes[f"command_{step}"] = (
                    fractal_tasks_mock_venv.parent
                    / "lib/python3.10/site-packages/fractal_tasks_mock"
                    / task.model_dump()[key]
                ).as_posix()

        task_dict[task_attributes["name"]] = task_attributes

    return task_dict


@pytest.fixture(scope="function")
def relink_python_interpreter_v2(fractal_tasks_mock_collection):
    """
    Rewire python executable in tasks
    """
    import os
    from pathlib import Path

    import logging
    from .fixtures_slurm import HAS_LOCAL_SBATCH

    if not HAS_LOCAL_SBATCH:

        logger = logging.getLogger("RELINK")
        logger.setLevel(logging.INFO)
        first_task = next(iter(fractal_tasks_mock_collection.values()))
        task_python = Path(first_task["command_non_parallel"].split()[0])
        logger.warning(f"Original tasks Python: {task_python.as_posix()}")

        actual_task_python = os.readlink(task_python)
        logger.warning(
            f"Actual tasks Python (after readlink): {actual_task_python}"
        )

        # NOTE that the docker container in the CI only has python3.9
        # installed, therefore we explicitly hardcode this version here, to
        # make debugging easier
        # NOTE that the slurm-node container also installs a version of
        # fractal-tasks-core
        task_python.unlink()
        new_actual_task_python = "/usr/bin/python3.9"
        task_python.symlink_to(new_actual_task_python)
        logger.warning(f"New tasks Python: {new_actual_task_python}")

        yield

        task_python.unlink()
        task_python.symlink_to(actual_task_python)
        logger.warning(
            f"Restored link from "
            f"{task_python.as_posix()} to {os.readlink(task_python)}"
        )
    else:
        yield
