import json
import logging
import shlex
import subprocess  # nosec
import sys
from pathlib import Path
from typing import Optional

import pytest
from devtools import debug  # noqa

from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.schemas.v2 import CollectionStatusV2
from fractal_server.app.schemas.v2 import ManifestV2
from fractal_server.app.schemas.v2 import TaskCollectCustomV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.utils import COLLECTION_LOG_FILENAME
from fractal_server.tasks.utils import get_collection_path
from fractal_server.tasks.utils import get_log_path
from tests.execute_command import execute_command


PREFIX = "api/v2/task"
INFO = sys.version_info
CURRENT_PYTHON = f"{INFO.major}.{INFO.minor}"


@pytest.mark.parametrize("payload_python_version", [CURRENT_PYTHON, None])
async def test_task_collection_from_wheel(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
    payload_python_version: Optional[str],
):
    # Note 1: Use function-scoped `FRACTAL_TASKS_DIR` to avoid sharing state.
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=CURRENT_PYTHON,
    )
    settings = Inject(get_settings)

    # Prepare absolute path to wheel file
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    payload_package = wheel_path.as_posix()

    # Prepare and validate payload
    payload = dict(package=payload_package, package_extras="my_extra")
    if payload_python_version is not None:
        payload["python_version"] = payload_python_version
        expected_python_version = payload_python_version
    else:
        expected_python_version = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    debug(payload)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state = res.json()
        state_id = state["id"]
        venv_path = state["data"]["venv_path"]
        debug(venv_path)
        assert "fractal-tasks-mock" in venv_path

        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        task_list = data["task_list"]
        for i, task in enumerate(task_list):
            if i == 0:
                assert task["meta_non_parallel"] == {"key1": "value1"}
                assert task["meta_parallel"] == {"key2": "value2"}
            else:
                assert task["meta_non_parallel"] == task["meta_parallel"] == {}
        assert data["status"] == "OK"
        # Check that log were written, even with CRITICAL logging level
        log = data["log"]
        assert log is not None
        # Check that my_extra was included, in a local-package collection
        assert ".whl[my_extra]" in log

        # Check on-disk files
        full_path = settings.FRACTAL_TASKS_DIR / venv_path
        assert get_collection_path(full_path).exists()
        assert get_log_path(full_path).exists()

        # Check actual Python version
        python_bin = task_list[0]["command_non_parallel"].split()[0]
        version = await execute_command(f"{python_bin} --version")
        assert expected_python_version in version

        # Check task source
        EXPECTED_SOURCE = (
            "pip_local:fractal_tasks_mock:0.0.1:my_extra:"
            f"py{expected_python_version}"
        )
        debug(EXPECTED_SOURCE)
        for task in task_list:
            debug(task["source"])
            assert task["source"].startswith(EXPECTED_SOURCE)

        # Check task type
        for task in task_list:
            if task["command_non_parallel"] is None:
                expected_type = "parallel"
            elif task["command_parallel"] is None:
                expected_type = "non_parallel"
            else:
                expected_type = "compound"
            assert task["type"] == expected_type

        # Check that argument JSON schemas are present
        for task in task_list:
            if task["command_non_parallel"] is not None:
                assert task["args_schema_non_parallel"] is not None
            if task["command_parallel"] is not None:
                assert task["args_schema_parallel"] is not None

        # Collect again (already installed)
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        assert data["info"] == "Already installed"

        # Check that *verbose* collection info contains logs
        res = await client.get(f"{PREFIX}/collect/{state_id}/?verbose=true")
        assert res.status_code == 200
        assert res.json()["data"]["log"] is not None

        # Modify a task source (via DB, since endpoint cannot modify source)
        db_task = await db.get(TaskV2, task_list[0]["id"])
        db_task.source = "EDITED_SOURCE"
        await db.commit()
        await db.close()

        # Collect again, and check that collection fails
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        debug(res.json())
        assert res.status_code == 422


async def test_task_collection_from_wheel_non_canonical(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
):
    """
    Same as test_task_collection_from_wheel, but package has a
    non-canonical name.
    """

    # Note 1: Use function-scoped `FRACTAL_TASKS_DIR` to avoid sharing state.
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=CURRENT_PYTHON,
    )
    # settings = Inject(get_settings)

    # Prepare absolute path to wheel file
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_non_canonical/dist"
        / "FrAcTaL_TaSkS_NoN_CaNoNiCaL-0.0.1-py3-none-any.whl"
    )
    payload_package = wheel_path.as_posix()

    # Prepare and validate payload
    payload = dict(package=payload_package, package_extras="my_extra")
    payload["python_version"] = CURRENT_PYTHON
    debug(payload)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == "pending"
        state = res.json()
        state_id = state["id"]
        venv_path = state["data"]["venv_path"]
        assert "fractal-tasks-non-canonical" in venv_path

        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        task_list = data["task_list"]

        # Verify how package name is used in source and folders
        assert "fractal_tasks_non_canonical" in task_list[0]["source"]
        python_path, task_path = task_list[0]["command_non_parallel"].split()
        assert (
            "FRACTAL_TASKS_DIR/.fractal/fractal-tasks-non-canonical0.0.1"
            in python_path
        )
        assert (
            "FRACTAL_TASKS_DIR/.fractal/fractal-tasks-non-canonical0.0.1"
            in task_path
        )
        assert "fractal_tasks_non_canonical" in task_path

        # Check that log were written, even with CRITICAL logging level
        log = data["log"]
        assert log is not None
        # Check that my_extra was included, in a local-package collection
        assert ".whl[my_extra]" in log
        assert data["status"] == "OK"


async def test_task_collection_from_pypi(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
):
    # Note 1: Use function-scoped `FRACTAL_TASKS_DIR` to avoid sharing state.
    # Note 2: Set logging level to CRITICAL, and then make sure that
    # task-collection logs are included
    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "FRACTAL_TASKS_DIR"),
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
        FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=CURRENT_PYTHON,
    )
    settings = Inject(get_settings)

    # Prepare and validate payload
    PACKAGE_VERSION = "1.0.2"
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    payload = dict(
        package="fractal-tasks-core",
        package_version=PACKAGE_VERSION,
        python_version=PYTHON_VERSION,
    )
    debug(payload)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state = res.json()
        state_id = state["id"]
        venv_path = state["data"]["venv_path"]
        debug(venv_path)
        assert "fractal-tasks-core" in venv_path

        # Get collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        task_list = data["task_list"]
        # Check that log were written, even with CRITICAL logging level
        log = data["log"]
        assert log is not None

        # Check on-disk files
        full_path = settings.FRACTAL_TASKS_DIR / venv_path
        assert get_collection_path(full_path).exists()
        assert get_log_path(full_path).exists()

        # Check actual Python version
        python_bin = task_list[0]["command_non_parallel"].split()[0]
        version = await execute_command(f"{python_bin} --version")
        assert PYTHON_VERSION in version

        # Check task source
        EXPECTED_SOURCE = (
            f"pip_remote:fractal_tasks_core:{PACKAGE_VERSION}::"
            f"py{PYTHON_VERSION}"
        )
        debug(EXPECTED_SOURCE)
        for task in task_list:
            debug(task["source"])
            assert task["source"].startswith(EXPECTED_SOURCE)

        # Check task type
        for task in task_list:
            if task["command_non_parallel"] is None:
                expected_type = "parallel"
            elif task["command_parallel"] is None:
                expected_type = "non_parallel"
            else:
                expected_type = "compound"
            assert task["type"] == expected_type

        # Check that argument JSON schemas are present
        for task in task_list:
            if task["command_non_parallel"] is not None:
                assert task["args_schema_non_parallel"] is not None
            if task["command_parallel"] is not None:
                assert task["args_schema_parallel"] is not None

        # Collect again (already installed)
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        assert data["info"] == "Already installed"

        # Check that *verbose* collection info contains logs
        res = await client.get(f"{PREFIX}/collect/{state_id}/?verbose=true")
        assert res.status_code == 200
        assert res.json()["data"]["log"] is not None

        # Modify a task source (via DB, since endpoint cannot modify source)
        db_task = await db.get(TaskV2, task_list[0]["id"])
        db_task.source = "EDITED_SOURCE"
        await db.commit()
        await db.close()

        # Collect again, and check that collection fails
        res = await client.post(f"{PREFIX}/collect/pip/", json=payload)
        debug(res.json())
        assert res.status_code == 422


async def test_read_log_from_file(db, tmp_path, MockCurrentUser, client):

    LOG = "fractal is awesome"
    with open(tmp_path / COLLECTION_LOG_FILENAME, "w") as f:
        f.write(LOG)
    state = CollectionStateV2(data=dict(venv_path=tmp_path.as_posix()))
    db.add(state)
    await db.commit()
    await db.refresh(state)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.get(f"{PREFIX}/collect/{state.id}/?verbose=true")

    assert res.json()["data"]["log"] == LOG

    state2 = CollectionStateV2()
    db.add(state2)
    await db.commit()
    await db.refresh(state2)
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.get(f"{PREFIX}/collect/{state2.id}/?verbose=true")
    assert res.status_code == 422
    assert res.json()["detail"] == (
        f"No 'venv_path' in CollectionStateV2[{state2.id}].data"
    )


async def test_task_collection_custom(
    client,
    MockCurrentUser,
    tmp_path: Path,
    testdata_path,
    task_factory,
):
    package_name = "fractal_tasks_mock"
    venv_name = f"venv_{package_name}"
    venv_path = (tmp_path / venv_name).as_posix()
    subprocess.run(shlex.split(f"{sys.executable} -m venv {venv_path}"))
    python_bin = (tmp_path / venv_name / "bin/python").as_posix()

    manifest_file = (
        testdata_path.parent
        / f"v2/{package_name}/src/{package_name}/__FRACTAL_MANIFEST__.json"
    ).as_posix()
    with open(manifest_file, "r") as f:
        manifest_dict = json.load(f)
    manifest = ManifestV2(**manifest_dict)

    # ---

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):

        payload_name = TaskCollectCustomV2(
            manifest=manifest,
            python_interpreter=python_bin,
            source="source1",
            package_root=None,
            package_name=package_name,
            version=None,
        )

        # Fail because no package is installed

        res = await client.post(
            f"{PREFIX}/collect/custom/",
            json=payload_name.dict(),
        )
        assert res.status_code == 422
        assert "Cannot determine 'package_root'" in res.json()["detail"]

        # Install
        wheel_file = (
            testdata_path.parent
            / f"v2/{package_name}/dist/{package_name}-0.0.1-py3-none-any.whl"
        ).as_posix()
        subprocess.run(
            shlex.split(f"{python_bin} -m pip install {wheel_file}")
        )

        # Success with 'package_name'
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_name.dict()
        )
        assert res.status_code == 201

        # Success with package_name with hypens instead of underscore
        payload_name.package_name = "fractal-tasks-mock"
        payload_name.source = "source2"
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_name.dict()
        )
        assert res.status_code == 201

        # Success with package_root
        package_name_underscore = package_name.replace("-", "_")
        python_command = (
            "import importlib.util; "
            "from pathlib import Path; "
            "init_path=importlib.util.find_spec"
            f'("{package_name_underscore}").origin; '
            "print(Path(init_path).parent.as_posix())"
        )
        res = subprocess.run(  # nosec
            shlex.split(f"{python_bin} -c '{python_command}'"),
            capture_output=True,
            encoding="utf8",
        )
        package_root = Path(res.stdout.strip("\n")).as_posix()

        payload_root = TaskCollectCustomV2(
            manifest=manifest,
            python_interpreter=python_bin,
            source="source3",
            package_root=package_root,
            package_name=None,
            version=None,
        )
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 201

        # Fail because same 'source'
        # V2
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 422
        assert "TaskV2" in res.json()["detail"]
        assert "already has source" in res.json()["detail"]
        # V1
        payload_root.source = "source4"
        await task_factory(source="test01:source4:create_ome_zarr_compound")
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 422
        assert "TaskV1" in res.json()["detail"]
        assert "already has source" in res.json()["detail"]

        # Fail because python_interpreter does not exist
        payload_root.python_interpreter = "/foo/bar"
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 422
        assert "doesn't exist or is not a file" in res.json()["detail"]

        # Fail because package_root does not exist
        payload_root.python_interpreter = sys.executable
        payload_root.package_root = "/foo/bar"
        res = await client.post(
            f"{PREFIX}/collect/custom/", json=payload_root.dict()
        )
        assert res.status_code == 422
        assert "doesn't exist or is not a directory" in res.json()["detail"]


async def test_task_collection_custom_fail_with_ssh(
    client,
    MockCurrentUser,
    override_settings_factory,
    testdata_path,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm_ssh")
    manifest_file = (
        testdata_path.parent
        / "v2/fractal_tasks_mock"
        / "src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json"
    ).as_posix()

    with open(manifest_file, "r") as f:
        manifest_dict = json.load(f)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/custom/",
            json=TaskCollectCustomV2(
                manifest=ManifestV2(**manifest_dict),
                python_interpreter="/may/not/exist",
                source="b",
                package_root=None,
                package_name="c",
            ).dict(),
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot infer 'package_root' with 'slurm_ssh' backend."
        )
