import shutil
from pathlib import Path

from devtools import debug  # noqa


PREFIX = "api/v2/task"


async def test_failed_get_collection_info(client, MockCurrentUser):
    """
    Get task-collection info for non-existing collection.
    """
    invalid_state_id = 99999
    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/collect/{invalid_state_id}/")
    debug(res)
    assert res.status_code == 404


async def test_collection_non_verified_user(client, MockCurrentUser):
    """
    Test that non-verified users are not authorized to make calls
    to `/api/v1/task/collect/pip/`.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(
            f"{PREFIX}/collect/pip/", json={"package": "fractal-tasks-core"}
        )
        assert res.status_code == 401


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
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 422
        assert "not supported" in res.json()["detail"]

    task_collection = dict(
        package=dummy_task_package_missing_manifest.as_posix()
    )
    debug(dummy_task_package_missing_manifest)
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 422
        assert "does not include" in res.json()["detail"]


async def test_failed_collection_missing_wheel_file(
    client,
    MockCurrentUser,
    tmp_path: Path,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package=str(tmp_path / "missing_file.whl")),
        )
        debug(res)
        debug(res.json())
        assert res.status_code == 422
        assert "does not exist" in str(res.json())


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

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(f"{PREFIX}/collect/pip/", json=task_collection)
        debug(res.json())
        assert res.status_code == 201
        assert res.json()["data"]["status"] == "pending"
        state = res.json()
        data = state["data"]
        assert "my-tasks-fail" in data["venv_path"]

        res = await client.get(f"{PREFIX}/collect/{state['id']}/?verbose=True")
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

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
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
        res = await client.get(f"{PREFIX}/collect/{collection_state_id}/")
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
