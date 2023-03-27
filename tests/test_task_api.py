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

PREFIX = "/api/v1/task"


async def test_task_get_list(db, client, task_factory, MockCurrentUser):
    t0 = await task_factory(name="task0")
    t1 = await task_factory(name="task1")
    t2 = await task_factory(index=2, subtask_list=[t0, t1])

    async with MockCurrentUser(persist=True):
        res = await client.get(f"{PREFIX}/")
        data = res.json()
        assert res.status_code == 200
        debug(data)
        assert len(data) == 3
        assert data[2]["id"] == t2.id


async def test_background_collection(
    db, registered_client, dummy_task_package
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called on it
    THEN the tasks are collected and the state is updated to db accordingly
    """
    task_pkg = _TaskCollectPip(package=dummy_task_package.as_posix())
    venv_path = create_package_dir_pip(
        task_pkg=task_pkg, user="test_bg_collection"
    )
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
        state=state, venv_path=venv_path, task_pkg=task_pkg, db=db
    )
    res = await registered_client.get(f"{PREFIX}/collect/{state.id}")
    debug(res.json())
    assert res.status_code == 200
    out_state = res.json()
    assert out_state["data"]["status"] == "OK"

    task_list = (await db.execute(select(Task))).scalars().all()
    debug(task_list)
    assert len(task_list) == 2


async def test_background_collection_failure(db, dummy_task_package):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called on it and it fails
    THEN
        * the log of the collection is saved to the state
        * the installation directory is removed
    """
    task_pkg = _TaskCollectPip(package=dummy_task_package.as_posix())
    venv_path = create_package_dir_pip(
        task_pkg=task_pkg, user="test_bg_collection_fail"
    )
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
        state=state, venv_path=venv_path, task_pkg=task_pkg, db=db
    )

    await db.refresh(state)
    debug(state)
    assert state.data["log"]
    assert state.data["status"] == "fail"
    assert state.data["info"].startswith("Original error")
    assert not venv_path.exists()


@pytest.mark.parametrize(
    ("slurm_user", "python_version"),
    [
        ("user00", None),
        pytest.param(
            "user01",
            "3.10",
            marks=pytest.mark.skipif(
                not shutil_which("python3.10"), reason="No python3.10 on host"
            ),
        ),
    ],
)
async def test_collection_api(
    client, dummy_task_package, MockCurrentUser, slurm_user, python_version
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

    task_collection = dict(
        package=dummy_task_package.as_posix(), python_version=python_version
    )

    async with MockCurrentUser(user_kwargs=dict(slurm_user=slurm_user)):
        # NOTE: collecting private tasks so that they are assigned to user and
        # written in a non-default folder. Bypass for non stateless
        # FRACTAL_TASKS_DIR in test suite.
        res = await client.post(
            f"{PREFIX}/collect/pip/?public=false", json=task_collection
        )
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
        assert data["log"] is None

        task_names = (t["name"] for t in task_list)
        assert len(task_list) == 2
        assert "dummy" in task_names
        assert "dummy parallel" in task_names

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

        # collect again
        res = await client.post(
            f"{PREFIX}/collect/pip/?public=false", json=task_collection
        )
        debug(res.json())
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        assert data["info"] == "Already installed"


async def test_collection_api_invalid_manifest(
    client, dummy_task_package_invalid_manifest, MockCurrentUser
):
    """
    GIVEN a package in a format that `pip` understands, with invalid manifest
    WHEN the api to collect tasks from that package is called
    THEN it returns 422 (Unprocessable Entity)
    """

    task_collection = dict(
        package=dummy_task_package_invalid_manifest.as_posix()
    )

    async with MockCurrentUser(persist=True):
        # NOTE: collecting private tasks so that they are assigned to user and
        # written in a non-default folder. Bypass for non stateless
        # FRACTAL_TASKS_DIR in test suite.
        res = await client.post(
            f"{PREFIX}/collect/pip/?public=false", json=task_collection
        )
        debug(res.json())
        assert res.status_code == 422


async def test_post_task(client, MockCurrentUser):
    async with MockCurrentUser(persist=True):

        # Successful task creation
        task = TaskCreate(
            name="task_name",
            command="task_command",
            source="task_source",
            input_type="task_input_type",
            output_type="task_output_type",
        )
        res = await client.post(f"{PREFIX}/", json=dict(task))
        debug(res.json())
        assert res.status_code == 201

        # Fail for repeated task.source
        new_task = TaskCreate(
            name="new_task_name",
            command="new_task_command",
            source="task_source",  # same source as `task`
            input_type="new_task_input_type",
            output_type="new_task_output_type",
        )
        res = await client.post(f"{PREFIX}/", json=dict(new_task))
        debug(res.json())
        assert res.status_code == 422

        # Fail for wrong payload
        res = await client.post(f"{PREFIX}/")  # request without body
        debug(res.json())
        assert res.status_code == 422


async def test_patch_task(
    db,
    registered_client,
    registered_superuser_client,
    task_factory,
    MockCurrentUser,
):
    task = await task_factory(name="task")
    old_source = task.source
    debug(task)
    NEW_NAME = "new name"
    NEW_INPUT_TYPE = "new input_type"
    NEW_OUTPUT_TYPE = "new output_type"
    NEW_COMMAND = "new command"
    NEW_SOURCE = "new source"
    NEW_DEFAULT_ARGS = {"key1": 1, "key2": 2}
    NEW_META = {"key3": "3", "key4": "4"}
    update = TaskUpdate(
        name=NEW_NAME,
        input_type=NEW_INPUT_TYPE,
        output_type=NEW_OUTPUT_TYPE,
        command=NEW_COMMAND,
        default_args=NEW_DEFAULT_ARGS,
        meta=NEW_META,
        source=NEW_SOURCE,
    )

    # Test non-superuser
    res = await registered_client.patch(
        f"{PREFIX}/{task.id}", json=update.dict(exclude_unset=True)
    )
    debug(res, res.json())
    assert res.status_code == 403

    # Test payload with `source`
    res = await registered_superuser_client.patch(
        f"{PREFIX}/{task.id}", json=update.dict(exclude_unset=True)
    )
    debug(res, res.json())
    assert res.status_code == 422

    # Test successuful (superuser)
    res = await registered_superuser_client.patch(
        f"{PREFIX}/{task.id}",
        json=update.dict(exclude_unset=True, exclude={"source"}),
    )
    assert res.status_code == 200
    assert res.json()["name"] == NEW_NAME
    assert res.json()["input_type"] == NEW_INPUT_TYPE
    assert res.json()["output_type"] == NEW_OUTPUT_TYPE
    assert res.json()["command"] == NEW_COMMAND
    assert res.json()["default_args"] == NEW_DEFAULT_ARGS
    assert res.json()["meta"] == NEW_META
    assert res.json()["source"] == old_source

    # Test dictionaries update
    OTHER_DEFAULT_ARGS = {"key1": 42, "key100": 100}
    OTHER_META = {"key4": [4, 8, 15], "key0": [16, 23, 42]}
    second_update = TaskUpdate(
        default_args=OTHER_DEFAULT_ARGS,
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
    assert len(res.json()["default_args"]) == 3
    assert len(res.json()["meta"]) == 3


async def test_task_collection_api_failure(
    client, MockCurrentUser, testdata_path
):
    """
    Try to collect a task package which triggers an error (namely its manifests
    includes a task for which there does not exist the python script), and
    handle failure.
    """

    path = str(
        testdata_path
        / "my-tasks-fail/dist/my_tasks_fail-0.1.0-py3-none-any.whl"
    )
    task_collection = dict(package=path)

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
