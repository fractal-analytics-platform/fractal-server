import json
import logging
import os
import sys
from pathlib import Path

import pytest
from v2_mock_models import TaskV2Mock

from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.runner.v2._local import FractalThreadPoolExecutor


@pytest.fixture()
def executor():
    with FractalThreadPoolExecutor() as e:
        yield e


def _run_cmd(*, cmd: str, label: str) -> str:
    import subprocess  # nosec
    import shlex

    res = subprocess.run(
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if not res.returncode == 0:
        logging.error(f"[{label}] FAIL")
        logging.error(f"[{label}] command: {cmd}")
        logging.error(f"[{label}] stdout: {res.stdout}")
        logging.error(f"[{label}] stderr: {res.stderr}")
        raise ValueError(res)
    return res.stdout


@pytest.fixture
def fractal_tasks_mock_no_db(
    fractal_tasks_mock_collection: dict[str, TaskV2],
) -> dict[str, TaskV2Mock]:
    """
    We use this fixture in tests that operate on Mock models,
    and therefore do not need the object to be in the database.
    """
    return {
        task.name: TaskV2Mock(id=_id, **task.dict())
        for _id, task in enumerate(fractal_tasks_mock_collection["task_list"])
    }


@pytest.fixture
def fractal_tasks_mock_venv_legacy(testdata_path, tmp_path_factory) -> dict:
    from v2_mock_models import TaskV1Mock

    basetemp = tmp_path_factory.getbasetemp()
    venv_name = "venv_fractal_tasks_core_alpha"
    venv_path = (basetemp / venv_name).as_posix()
    python_bin = (basetemp / venv_name / "bin/python").as_posix()

    if not os.path.isdir(venv_path):
        logging.debug(f"venv does not exists ({venv_path=})")
        # Create venv
        cmd = f"{sys.executable} -m venv {venv_path}"
        _run_cmd(cmd=cmd, label="create-venv")
        # Install fractal-tasks-core-alpha from wheel
        wheel_file = (
            testdata_path
            / "dummy_package_with_args_schemas"
            / "dist/fractal_tasks_core_alpha-0.0.1a0-py3-none-any.whl"
        ).as_posix()
        cmd = f"{python_bin} -m pip install {wheel_file}"
        _run_cmd(cmd=cmd, label="install-fractal-tasks-core-alpha")
    else:
        logging.info("venv already exists")

    # Extract installed-package folder
    cmd = f"{python_bin} -m pip show fractal_tasks_core_alpha"
    out = _run_cmd(cmd=cmd, label="extract-pkg-dir")
    location = next(
        line for line in out.split("\n") if line.startswith("Location:")
    )
    location = location.replace("Location: ", "")
    src_dir = Path(location) / "fractal_tasks_core_alpha/"

    # Construct TaskV1Mock objects, and store them as a key-value pairs
    # (indexed by their names)
    with (src_dir / "__FRACTAL_MANIFEST__.json").open("r") as f:
        manifest = json.load(f)
    task_dict = {}
    for ind, task in enumerate(manifest["task_list"]):

        more_attributes = {}
        if task["name"] == "dummy parallel":
            more_attributes["meta"] = {"parallelization_level": "image"}

        task_path = (src_dir / task["executable"]).as_posix()
        t = TaskV1Mock(
            id=ind,
            name=task["name"],
            source=task["name"].replace(" ", "_"),
            input_type="Any",
            output_type="Any",
            command=f"{python_bin} {task_path}",
            **more_attributes,
        )
        task_dict[t.name] = t
    return task_dict
