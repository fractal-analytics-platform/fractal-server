import json
import logging
import os
import sys
from pathlib import Path

import pytest

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
def fractal_tasks_mock_venv(
    fractal_tasks_mock_venv_new: Path,
    fractal_tasks_mock_collection_new: list[TaskV2],
) -> dict:
    from v2_mock_models import TaskV2Mock

    task_dict = {}
    for ind, task in enumerate(fractal_tasks_mock_collection_new):
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
                    fractal_tasks_mock_venv_new.parent
                    / "lib/python3.10/site-packages/fractal_tasks_mock"
                    / task.model_dump()[key]
                ).as_posix()

        t = TaskV2Mock(**task_attributes)
        task_dict[t.name] = t

    return task_dict


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
