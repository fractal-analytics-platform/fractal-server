import logging
import os
import time
from pathlib import Path
from shutil import which as shutil_which

import pytest
from devtools import debug
from sqlmodel import select

from .fixtures_tasks import execute_command
from fractal_server.app.api.v1.task import _background_collect_pip
from fractal_server.app.api.v1.task import _TaskCollectPip
from fractal_server.app.api.v1.task import create_package_dir_pip
from fractal_server.app.api.v1.task import TaskCollectStatus
from fractal_server.app.models import State
from fractal_server.app.models import Task
from fractal_server.common.schemas.task import TaskCreate
from fractal_server.common.schemas.task import TaskUpdate
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
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


async def test_task_get_list(db, client, task_factory, MockCurrentUser):
    t0 = await task_factory(name="task0", source="source0")
    t1 = await task_factory(name="task1", source="source1")
    t2 = await task_factory(index=2, subtask_list=[t0, t1])

    async with MockCurrentUser(persist=True):
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert res.status_code == 200
        debug(data)
        assert len(data) == 3
        assert data[2]["id"] == t2.id


async def test_background_collection(
    db,
    client,
    MockCurrentUser,
    dummy_task_package,
    override_settings_factory,
    tmp_path,
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called on it
    THEN the tasks are collected and the state is updated to db accordingly
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_background_collection")
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
    debug(state)
    await _background_collect_pip(
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )
    async with MockCurrentUser(persist=True):
        res = await client.get(f"{PREFIX}/collect/{state.id}")
    debug(res.json())
    assert res.status_code == 200
    out_state = res.json()
    assert out_state["data"]["status"] == "OK"

    task_list = (await db.execute(select(Task))).scalars().all()
    debug(task_list)
    assert len(task_list) == 2


@pytest.mark.parametrize(
    "level", [logging.DEBUG, logging.INFO, logging.WARNING]
)
async def test_background_collection_logs(
    db,
    client,
    MockCurrentUser,
    dummy_task_package,
    override_settings_factory,
    tmp_path,
    level: int,
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called, for a given FRACTAL_LOGGING_LEVEL
    THEN the logs are always present
    """
    override_settings_factory(
        FRACTAL_LOGGING_LEVEL=level,
        FRACTAL_TASKS_DIR=(
            tmp_path / f"test_background_collection_logs_{level}"
        ),
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


async def test_background_collection_failure(
    db, dummy_task_package, override_settings_factory, tmp_path
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called on it and it fails
    THEN
        * the log of the collection is saved to the state
        * the installation directory is removed
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_background_collection_failure")
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


async def test_collection_api_missing_file(
    client,
    MockCurrentUser,
    tmp_path,
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


async def test_collection_api_local_package_with_extras(
    client,
    MockCurrentUser,
    dummy_task_package,
    override_settings_factory,
    tmp_path,
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
async def test_collection_api(
    db,
    client,
    dummy_task_package,
    MockCurrentUser,
    python_version,
    override_settings_factory,
    tmp_path,
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

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_collection_api")
    )

    task_collection = dict(package=dummy_task_package.as_posix())
    PKG_SOURCE = "pip_local:fractal_tasks_dummy:0.1.0::"
    if python_version:
        task_collection["python_version"] = python_version
        PKG_SOURCE = f"pip_local:fractal_tasks_dummy:0.1.0::py{python_version}"
    debug(PKG_SOURCE)

    async with MockCurrentUser():
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["data"]["status"] == "pending"

        state = res.json()
        data = state["data"]
        assert "fractal_tasks_dummy" in data["venv_path"]
        venv_path = Path(data["venv_path"])

        res = await client.get(f"{PREFIX}/collect/{state['id']}")
        debug(res.json())
        assert res.status_code == 200
        state = res.json()
        data = state["data"]

        assert data["status"] == "OK"
        task_list = data["task_list"]
        assert data["log"]

        task_names = (t["name"] for t in task_list)
        assert len(task_list) == 2
        assert "dummy" in task_names
        assert "dummy parallel" in task_names

        assert task_list[0]["source"] == f"{PKG_SOURCE}:dummy"
        assert task_list[1]["source"] == f"{PKG_SOURCE}:dummy_parallel"

        # using verbose option
        res = await client.get(f"{PREFIX}/collect/{state['id']}?verbose=true")
        debug(res.json())
        state = res.json()
        data = state["data"]
        assert res.status_code == 200
        assert data["log"] is not None

        # check status of non-existing collection
        invalid_state_id = 99999
        res = await client.get(f"{PREFIX}/collect/{invalid_state_id}")
        debug(res)
        assert res.status_code == 404

        settings = Inject(get_settings)
        full_path = settings.FRACTAL_TASKS_DIR / venv_path
        assert get_collection_path(full_path).exists()
        assert get_log_path(full_path).exists()
        if python_version:
            python_bin = data["task_list"][0]["command"].split()[0]
            version = await execute_command(f"{python_bin} --version")
            assert python_version in version

        # Collect again
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        assert data["info"] == "Already installed"

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
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 422


async def test_collection_api_invalid_manifest(
    client,
    dummy_task_package_invalid_manifest,
    dummy_task_package_missing_manifest,
    MockCurrentUser,
    override_settings_factory,
    tmp_path,
):
    """
    GIVEN a package with invalid/missing manifest
    WHEN the api to collect tasks from that package is called
    THEN it returns 422 (Unprocessable Entity) with an informative message
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_collection_api_invalid_manifest")
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


async def test_post_task(client, MockCurrentUser):

    async with MockCurrentUser(persist=True) as user:
        TASK_OWNER = user.username or user.slurm_user
        TASK_SOURCE = "some_source"

        # Successful task creation
        VERSION = "1.2.3"
        task = TaskCreate(
            name="task_name",
            command="task_command",
            source=TASK_SOURCE,
            input_type="task_input_type",
            output_type="task_output_type",
            version=VERSION,
        )
        payload = task.dict(exclude_unset=True)
        res = await client.post(f"{PREFIX}/", json=payload)
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["version"] == VERSION
        assert res.json()["owner"] == TASK_OWNER
        assert res.json()["source"] == f"{TASK_OWNER}:{TASK_SOURCE}"

        # Fail for repeated task.source
        new_task = TaskCreate(
            name="new_task_name",
            command="new_task_command",
            source=TASK_SOURCE,  # same source as `task`
            input_type="new_task_input_type",
            output_type="new_task_output_type",
        )
        payload = new_task.dict(exclude_unset=True)
        res = await client.post(f"{PREFIX}/", json=payload)
        debug(res.json())
        assert res.status_code == 422

        # Fail for wrong payload
        res = await client.post(f"{PREFIX}/")  # request without body
        debug(res.json())
        assert res.status_code == 422

    # Test multiple combinations of (username, slurm_user)
    SLURM_USER = "some_slurm_user"
    USERNAME = "some_username"
    # Case 1: (username, slurm_user) = (None, None)
    user_kwargs = dict(username=None, slurm_user=None)
    payload["source"] = "source_1"
    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Cannot add a new task because current user does not have "
            "`username` or `slurm_user` attributes."
        )
    # Case 2: (username, slurm_user) = (not None, not None)
    user_kwargs = dict(username=USERNAME, slurm_user=SLURM_USER)
    payload["source"] = "source_2"
    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == USERNAME
    # Case 3: (username, slurm_user) = (None, not None)
    user_kwargs = dict(username=None, slurm_user=SLURM_USER)
    payload["source"] = "source_3"
    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == SLURM_USER
    # Case 4: (username, slurm_user) = (not None, None)
    user_kwargs = dict(username=USERNAME, slurm_user=None)
    payload["source"] = "source_4"
    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs):
        res = await client.post(f"{PREFIX}/", json=payload)
        assert res.status_code == 201
        assert res.json()["owner"] == USERNAME


async def test_patch_task_auth(
    MockCurrentUser,
    client,
    task_factory,
):
    """
    GIVEN a Task `A` with owner Alice and a Task `N` with owner None
    WHEN Alice, Bob and a Superuser try to patch them
    THEN Alice can edit `A`, Bob cannot edit anything
         and the Superuser can edit both A and N.
    """
    USER_1 = "Alice"
    USER_2 = "Bob"

    task_with_no_owner = await task_factory()
    task_with_no_owner_id = task_with_no_owner.id

    async with MockCurrentUser(user_kwargs={"username": USER_1}):

        task = TaskCreate(
            name="task_name",
            command="task_command",
            source="task_source",
            input_type="task_input_type",
            output_type="task_output_type",
        )
        res = await client.post(
            f"{PREFIX}/", json=task.dict(exclude_unset=True)
        )
        assert res.status_code == 201
        assert res.json()["owner"] == USER_1

        task_id = res.json()["id"]

        # Test success: owner == user
        update = TaskUpdate(name="new_name_1")
        res = await client.patch(
            f"{PREFIX}/{task_id}", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_1"

    async with MockCurrentUser(user_kwargs={"slurm_user": USER_2}):

        update = TaskUpdate(name="new_name_2")

        # Test fail: (not user.is_superuser) and (owner != user)
        res = await client.patch(
            f"{PREFIX}/{task_id}", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 403
        assert res.json()["detail"] == (
            f"Current user ({USER_2}) cannot modify task ({task_id}) "
            f"with different owner ({USER_1})."
        )

        # Test fail: (not user.is_superuser) and (owner == None)
        res = await client.patch(
            f"{PREFIX}/{task_with_no_owner_id}",
            json=update.dict(exclude_unset=True),
        )
        assert res.status_code == 403
        assert res.json()["detail"] == (
            "Only a superuser can edit a task with `owner=None`."
        )

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):

        res = await client.get(f"{PREFIX}/{task_id}")
        assert res.json()["name"] == "new_name_1"

        # Test success: (owner != user) but (user.is_superuser)
        update = TaskUpdate(name="new_name_3")
        res = await client.patch(
            f"{PREFIX}/{task_id}", json=update.dict(exclude_unset=True)
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_3"

        # Test success: (owner == None) but (user.is_superuser)
        update = TaskUpdate(name="new_name_4")
        res = await client.patch(
            f"{PREFIX}/{task_with_no_owner_id}",
            json=update.dict(exclude_unset=True),
        )
        assert res.status_code == 200
        assert res.json()["name"] == "new_name_4"


async def test_patch_task(
    db,
    registered_client,
    registered_superuser_client,
    task_factory,
):
    task = await task_factory(name="task")

    old_source = task.source
    debug(task)
    NEW_NAME = "new name"
    NEW_INPUT_TYPE = "new input_type"
    NEW_OUTPUT_TYPE = "new output_type"
    NEW_COMMAND = "new command"
    NEW_SOURCE = "new source"
    NEW_META = {"key3": "3", "key4": "4"}
    NEW_VERSION = "1.2.3"
    update = TaskUpdate(
        name=NEW_NAME,
        input_type=NEW_INPUT_TYPE,
        output_type=NEW_OUTPUT_TYPE,
        command=NEW_COMMAND,
        source=NEW_SOURCE,
        meta=NEW_META,
        version=NEW_VERSION,
    )

    # Test fails with `source`
    res = await registered_superuser_client.patch(
        f"{PREFIX}/{task.id}", json=update.dict(exclude_unset=True)
    )
    debug(res, res.json())
    assert res.status_code == 422
    assert res.json()["detail"] == "patch_task endpoint cannot set `source`"

    # Test successuful without `source`
    res = await registered_superuser_client.patch(
        f"{PREFIX}/{task.id}",
        json=update.dict(exclude_unset=True, exclude={"source"}),
    )
    assert res.status_code == 200
    assert res.json()["name"] == NEW_NAME
    assert res.json()["input_type"] == NEW_INPUT_TYPE
    assert res.json()["output_type"] == NEW_OUTPUT_TYPE
    assert res.json()["command"] == NEW_COMMAND
    assert res.json()["meta"] == NEW_META
    assert res.json()["source"] == old_source
    assert res.json()["version"] == NEW_VERSION
    assert res.json()["owner"] is None

    # Test dictionaries update
    OTHER_META = {"key4": [4, 8, 15], "key0": [16, 23, 42]}
    second_update = TaskUpdate(
        meta=OTHER_META,
    )
    res = await registered_superuser_client.patch(
        f"{PREFIX}/{task.id}",
        json=second_update.dict(exclude_unset=True),
    )
    debug(res, res.json())
    assert res.status_code == 200
    assert res.json()["name"] == NEW_NAME
    assert res.json()["input_type"] == NEW_INPUT_TYPE
    assert res.json()["output_type"] == NEW_OUTPUT_TYPE
    assert res.json()["command"] == NEW_COMMAND
    assert len(res.json()["meta"]) == 3


@pytest.mark.parametrize("username", (None, "myself"))
@pytest.mark.parametrize("slurm_user", (None, "myself_slurm"))
@pytest.mark.parametrize("owner", (None, "another_owner"))
async def test_patch_task_different_users(
    db,
    registered_superuser_client,
    task_factory,
    username,
    slurm_user,
    owner,
):
    """
    Test that the `username` or `slurm_user` attributes of a (super)user do not
    affect their ability to patch a task. They do raise warnings, but the PATCH
    endpoint returns correctly.
    """

    task = await task_factory(name="task", owner=owner)
    debug(task)
    assert task.owner == owner

    # Update user
    payload = {}
    if username:
        payload["username"] = username
    if slurm_user:
        payload["slurm_user"] = slurm_user
    if payload:
        res = await registered_superuser_client.patch(
            "/auth/users/me",
            json=payload,
        )
        debug(res.json())
        assert res.status_code == 200

    # Patch task
    NEW_NAME = "new name"
    payload = TaskUpdate(name=NEW_NAME).dict(exclude_unset=True)
    debug(payload)
    res = await registered_superuser_client.patch(
        f"{PREFIX}/{task.id}", json=payload
    )
    debug(res.json())
    assert res.status_code == 200
    assert res.json()["owner"] == owner
    if username:
        assert res.json()["owner"] != username
    if slurm_user:
        assert res.json()["owner"] != slurm_user


async def test_task_collection_api_failure(
    client, MockCurrentUser, testdata_path, override_settings_factory, tmp_path
):
    """
    Try to collect a task package which triggers an error (namely its manifests
    includes a task for which there does not exist the python script), and
    handle failure.
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=(tmp_path / "test_task_collection_api_failure")
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


async def test_get_task(task_factory, client, MockCurrentUser):
    async with MockCurrentUser():
        task = await task_factory(name="name")
        res = await client.get(f"{PREFIX}/{task.id}")
        debug(res)
        debug(res.json())
        assert res.status_code == 200


async def test_background_collection_with_json_schemas(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path,
    testdata_path,
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
