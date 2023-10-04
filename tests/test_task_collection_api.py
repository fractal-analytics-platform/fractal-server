import logging
import os
import shutil
import time
from pathlib import Path
from shutil import which as shutil_which

import pytest
from devtools import debug
from sqlmodel import select

from .fixtures_tasks import execute_command
from fractal_server.app.api.v1.task_collection import _background_collect_pip
from fractal_server.app.api.v1.task_collection import _TaskCollectPip
from fractal_server.app.api.v1.task_collection import create_package_dir_pip
from fractal_server.app.api.v1.task_collection import TaskCollectStatus
from fractal_server.app.models import State
from fractal_server.app.models import Task
from fractal_server.config import get_settings
from fractal_server.tasks.collection import get_collection_path
from fractal_server.tasks.collection import get_log_path
from fractal_server.tasks.collection import inspect_package

PREFIX = "/api/v1/task"


def _inspect_package_and_set_attributes(task_pkg) -> None:
    """
    Reproduce a logical block that normally takes place in the task-collection
    endpoint
    """
    # Extract info form the wheel package (this is part of the endpoint)
    pkg_info = inspect_package(task_pkg.package_path)
    task_pkg.package_name = pkg_info["pkg_name"]
    task_pkg.package_version = pkg_info["pkg_version"]
    task_pkg.package_manifest = pkg_info["pkg_manifest"]
    task_pkg.check()


async def test_failed_get_collection_info(client, MockCurrentUser):
    """
    Get task-collection info for non-existing collection.
    """
    invalid_state_id = 99999
    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/collect/{invalid_state_id}")
    debug(res)
    assert res.status_code == 404


@pytest.mark.parametrize(
    "python_version",
    [
        None,
        pytest.param(
            "3.10",
            marks=pytest.mark.skipif(
                not shutil_which("python3.10"), reason="No python3.10 on host"
            ),
        ),
    ],
)
async def test_collection(
    db,
    client,
    dummy_task_package,
    MockCurrentUser,
    override_settings_factory,
    python_version,
    tmp_path: Path,
):
    """
    GIVEN a package in a format that `pip` understands
    WHEN the api to collect tasks from that package is called
    THEN
        * a dedicated directory is created and returned
        * in the background, an environment is created, the package is
          installed and the task collected
        * it is possible to GET the collection with the path to the folder to
          check the status of the background process
        * if called twice, the same tasks are returned without installing
    """

    override_settings_factory(FRACTAL_TASKS_DIR=(tmp_path / "test_collection"))

    # Prepare and validate payload
    task_pkg_dict = dict(package=str(dummy_task_package))
    debug(task_pkg_dict)
    _TaskCollectPip(**task_pkg_dict)

    # Prepare expecte source
    if python_version:
        task_pkg_dict["python_version"] = python_version
        EXPECTED_SOURCE = (
            f"pip_local:fractal_tasks_dummy:0.1.0::py{python_version}"
        )
    else:
        EXPECTED_SOURCE = "pip_local:fractal_tasks_dummy:0.1.0::"
    debug(EXPECTED_SOURCE)

    async with MockCurrentUser():

        # Trigger collection
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_pkg_dict)
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["data"]["status"] == "pending"
        state = res.json()
        state_id = state["id"]
        data = state["data"]
        venv_path = Path(data["venv_path"])
        assert "fractal_tasks_dummy" in data["venv_path"]

        # Get/check collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}")
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        task_list = data["task_list"]
        task_names = (t["name"] for t in task_list)
        assert data["status"] == "OK"
        assert data["log"]
        assert len(task_list) == 2
        assert "dummy" in task_names
        assert "dummy parallel" in task_names
        assert task_list[0]["source"] == f"{EXPECTED_SOURCE}:dummy"
        assert task_list[1]["source"] == f"{EXPECTED_SOURCE}:dummy_parallel"

        # Check on-disk files
        settings = get_settings()
        full_path = settings.FRACTAL_TASKS_DIR / venv_path
        assert get_collection_path(full_path).exists()
        assert get_log_path(full_path).exists()

        # Check source
        if python_version:
            python_bin = data["task_list"][0]["command"].split()[0]
            version = await execute_command(f"{python_bin} --version")
            assert python_version in version

        # Collect again (already installed)
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_pkg_dict)
        debug(res.json())
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        assert data["info"] == "Already installed"

        # Check that *verbose* collection info contains logs
        res = await client.get(f"{PREFIX}/collect/{state_id}?verbose=true")
        assert res.status_code == 200
        assert res.json()["data"]["log"] is not None

        # Edit a task (via DB, since endpoint cannot modify source)
        res = await client.get(f"{PREFIX}/")
        assert res.status_code == 200
        task_list = res.json()
        db_task = await db.get(Task, task_list[0]["id"])
        db_task.source = "some_new_source"
        await db.merge(db_task)
        await db.commit()
        await db.close()

        # Collect again, and check that collection fails
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_pkg_dict)
        debug(res.json())
        assert res.status_code == 422


async def test_collection_local_package_with_extras(
    client,
    MockCurrentUser,
    dummy_task_package,
    override_settings_factory,
    tmp_path: Path,
):
    """
    Check that the package extras are correctly included in a local-package
    collection.
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(
            tmp_path / "test_collection_api_local_package_with_extras"
        )
    )

    async with MockCurrentUser():
        # Task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                package=dummy_task_package.as_posix(),
                package_extras="my_extra",
            ),
        )
        debug(res.json())
        assert res.status_code == 201

        # Get logs
        res = await client.get(f"{PREFIX}/collect/{res.json()['id']}")
        debug(res.json())
        assert res.status_code == 200
        log = res.json()["data"]["log"]
        assert ".whl[my_extra]" in log


async def test_collection_with_json_schemas(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
):
    """
    GIVEN a package which
        1. has JSON Schemas for task arguments
        2. has the tasks in a `tasks` subpackage
    WHEN the background collection is called on it
    THEN
        1. The tasks are collected and the args_schema and args_schema_version
        attributes are not None.
        2. The task.command attribtues are correct (i.e. they point to existing
        task files)
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_background_collection")
    )

    task_package = (
        testdata_path
        / "dummy_package_with_args_schemas"
        / "dist/fractal_tasks_core_alpha-0.0.1a0-py3-none-any.whl"
    )
    task_pkg = _TaskCollectPip(package=task_package.as_posix())

    # Extract info form the wheel package (this would be part of the endpoint)
    _inspect_package_and_set_attributes(task_pkg)
    debug(task_pkg)

    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    collection_status = TaskCollectStatus(
        status="pending", venv_path=venv_path, package=task_pkg.package
    )
    # Replacing with path because of non-serializable Path
    collection_status_dict = collection_status.sanitised_dict()

    state = State(data=collection_status_dict)
    db.add(state)
    await db.commit()
    await db.refresh(state)
    debug(state)
    await _background_collect_pip(
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )
    async with MockCurrentUser(persist=True):
        status = "pending"
        while status == "pending":
            res = await client.get(f"{PREFIX}/collect/{state.id}")
            debug(res.json())
            assert res.status_code == 200
            status = res.json()["data"]["status"]
            time.sleep(0.5)
    assert status == "OK"

    task_list = (await db.execute(select(Task))).scalars().all()
    assert len(task_list) == 2
    for task in task_list:
        debug(task)
        assert task.args_schema is not None
        assert task.args_schema_version is not None

        # The fractal_tasks_core_alpha package has tasks in the
        # fractal_tasks_core_alpha/tasks subpackage. Here we check that these
        # paths are correctly set in the Task object
        task_file = task.command.split(" ")[1]
        debug(task_file)
        assert os.path.exists(task_file)
        assert os.path.isfile(task_file)
        # Check that docs_info and docs_link are correct
        assert task.docs_info in ["", "This is a parallel task"]
        assert task.docs_link in [None, "http://www.example.org"]


async def test_failed_collection_missing_wheel_file(
    client,
    MockCurrentUser,
    tmp_path: Path,
):
    async with MockCurrentUser():
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package=str(tmp_path / "missing_file.whl")),
        )
        debug(res)
        debug(res.json())
        assert res.status_code == 422
        assert "does not exist" in str(res.json())


async def test_failed_collection_invalid_manifest(
    client,
    dummy_task_package_invalid_manifest,
    dummy_task_package_missing_manifest,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
):
    """
    GIVEN a package with invalid/missing manifest
    WHEN the api to collect tasks from that package is called
    THEN it returns 422 (Unprocessable Entity) with an informative message
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(
            tmp_path / "test_failed_collection_invalid_manifest"
        )
    )

    task_collection = dict(
        package=dummy_task_package_invalid_manifest.as_posix()
    )
    debug(dummy_task_package_invalid_manifest)
    async with MockCurrentUser(persist=True):
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 422
        assert "not supported" in res.json()["detail"]

    task_collection = dict(
        package=dummy_task_package_missing_manifest.as_posix()
    )
    debug(dummy_task_package_missing_manifest)
    async with MockCurrentUser(persist=True):
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 422
        assert "does not include" in res.json()["detail"]


async def test_failed_collection_missing_task_file(
    client,
    MockCurrentUser,
    override_settings_factory,
    testdata_path: Path,
    tmp_path: Path,
):
    """
    Try to collect a task package which triggers an error (namely its manifests
    includes a task for which there does not exist the python script), and
    handle failure.
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(
            tmp_path / "test_failed_collection_missing_task_file"
        )
    )
    debug(tmp_path)

    path = str(
        testdata_path
        / "my-tasks-fail/dist/my_tasks_fail-0.1.0-py3-none-any.whl"
    )
    task_collection = dict(package=path)
    debug(task_collection)

    async with MockCurrentUser():
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["data"]["status"] == "pending"
        state = res.json()
        data = state["data"]
        assert "my_tasks_fail" in data["venv_path"]

        res = await client.get(f"{PREFIX}/collect/{state['id']}?verbose=True")
        debug(res.json())

        assert res.status_code == 200
        state = res.json()
        data = state["data"]

        assert "Cannot find executable" in data["info"]
        assert data["status"] == "fail"
        assert data["log"]  # This is because of verbose=True
        assert "fail" in data["log"]


async def test_failed_collection_existing_db_tasks(
    client,
    MockCurrentUser,
    dummy_task_package,
    override_settings_factory,
    tmp_path: Path,
):
    """
    Catch issue 866:

    * collect tasks
    * remove venv folders
    * collect tasks again
    """

    _FRACTAL_TASKS_DIR = (
        tmp_path / "test_collection_api_local_package_with_extras"
    )
    override_settings_factory(FRACTAL_TASKS_DIR=_FRACTAL_TASKS_DIR)

    async with MockCurrentUser():

        # First task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                package=dummy_task_package.as_posix(),
                package_extras="my_extra",
            ),
        )
        collection_state_id = res.json()["id"]
        venv_path = res.json()["data"]["venv_path"]
        debug(collection_state_id)
        debug(venv_path)
        assert res.status_code == 201

        # Check that task collection is complete
        res = await client.get(f"{PREFIX}/collect/{collection_state_id}")
        assert res.status_code == 200
        debug(res.json())
        assert res.json()["data"]["status"] == "OK"

        # Remove task folder from disk
        shutil.rmtree(_FRACTAL_TASKS_DIR / venv_path)

        # Second task collection
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                package=dummy_task_package.as_posix(),
                package_extras="my_extra",
            ),
        )
        assert res.status_code == 422
        debug(res.json())
        assert "Task with source" in res.json()["detail"]
        assert "already exists in the database" in res.json()["detail"]


@pytest.mark.parametrize(
    "level", [logging.DEBUG, logging.INFO, logging.WARNING]
)
async def test_logs(
    db,
    client,
    MockCurrentUser,
    dummy_task_package,
    override_settings_factory,
    tmp_path: Path,
    level: int,
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called, for a given FRACTAL_LOGGING_LEVEL
    THEN the logs are always present
    """
    override_settings_factory(
        FRACTAL_LOGGING_LEVEL=level,
        FRACTAL_TASKS_DIR=(tmp_path / f"test_logs_{level}"),
    )

    task_pkg = _TaskCollectPip(package=dummy_task_package.as_posix())

    # Extract info form the wheel package (this would be part of the endpoint)
    _inspect_package_and_set_attributes(task_pkg)
    debug(task_pkg)

    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    debug(venv_path)
    collection_status = TaskCollectStatus(
        status="pending", venv_path=venv_path, package=task_pkg.package
    )
    # replacing with path because of non-serializable Path
    collection_status_dict = collection_status.sanitised_dict()

    state = State(data=collection_status_dict)
    db.add(state)
    await db.commit()
    await db.refresh(state)
    debug(state)
    await _background_collect_pip(
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )
    async with MockCurrentUser(persist=True):
        res = await client.get(f"{PREFIX}/collect/{state.id}")
    out_state = res.json()
    debug(out_state)
    assert res.status_code == 200
    assert out_state["data"]["status"] == "OK"
    debug(out_state["data"]["log"])
    debug(out_state["data"]["info"])
    assert out_state["data"]["log"]


async def test_logs_failed_collection(
    db, dummy_task_package, override_settings_factory, tmp_path: Path
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called on it and it fails
    THEN
        * the log of the collection is saved to the state
        * the installation directory is removed
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_logs_failed_collection")
    )

    task_pkg = _TaskCollectPip(package=dummy_task_package.as_posix())

    # Extract info form the wheel package (this would be part of the endpoint)
    _inspect_package_and_set_attributes(task_pkg)
    debug(task_pkg)

    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    collection_status = TaskCollectStatus(
        status="pending", venv_path=venv_path, package=task_pkg.package
    )
    # replacing with path because of non-serializable Path
    collection_status_dict = collection_status.sanitised_dict()
    state = State(data=collection_status_dict)
    db.add(state)
    await db.commit()
    await db.refresh(state)

    task_pkg.package = "__NO_PACKAGE"
    task_pkg.package_path = None

    await _background_collect_pip(
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )

    await db.refresh(state)
    debug(state)
    assert state.data["log"]
    assert state.data["status"] == "fail"
    assert state.data["info"].startswith("Original error")
    assert not venv_path.exists()
