import json
import os
from pathlib import Path

from devtools import debug  # noqa

from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip

PREFIX = "api/v2/task"


async def test_failed_API_calls(
    client, MockCurrentUser, tmp_path, testdata_path, task_factory_v2
):
    # Missing state ID
    invalid_state_id = 99999
    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/collect/{invalid_state_id}/")
        assert res.status_code == 404

    # Task collection triggered by non-verified user
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(
            f"{PREFIX}/collect/pip/", json={"package": "fractal-tasks-core"}
        )
        assert res.status_code == 401

    # Missing wheel file
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package=str(tmp_path / "missing_file.whl")),
        )
        assert res.status_code == 422
        assert "does not exist" in str(res.json())

    # Task collection fails if a task with the same source already exists
    # (see issue 866)
    await task_factory_v2(
        source="pip_local:fractal_tasks_mock:0.0.1:::create_ome_zarr_compound"
    )
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        wheel_path = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package=wheel_path.as_posix()),
        )
        assert res.status_code == 422
        assert "Task with source" in res.json()["detail"]
        assert "already exists in the database" in res.json()["detail"]


async def test_invalid_manifest(
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
):
    """
    GIVEN a package with invalid/missing manifest
    WHEN the api to collect tasks from that package is called
    THEN it returns 422 (Unprocessable Entity) with an informative message
    """

    override_settings_factory(FRACTAL_TASKS_DIR=tmp_path)
    debug(tmp_path)

    # Invalid manifest
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_fail/invalid_manifest"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/pip/", json=dict(package=wheel_path.as_posix())
        )
        debug(res.json())
        assert res.status_code == 422
        assert "not supported" in res.json()["detail"]

    # Missing manifest
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_fail/missing_manifest"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/pip/", json=dict(package=wheel_path.as_posix())
        )
        debug(res.json())
        assert res.status_code == 422
        assert "does not include" in res.json()["detail"]


async def test_missing_task_executable(
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
    override_settings_factory(FRACTAL_TASKS_DIR=tmp_path)

    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_fail/missing_executable"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger collection
        res = await client.post(
            f"{PREFIX}/collect/pip/", json=dict(package=wheel_path.as_posix())
        )
        assert res.status_code == 201
        assert res.json()["data"]["status"] == "pending"
        state_id = res.json()["id"]
        # Inspect collection outcome
        res = await client.get(f"{PREFIX}/collect/{state_id}/?verbose=True")
        debug(res.json())
        assert res.status_code == 200
        data = res.json()["data"]
        assert "Cannot find executable" in data["info"]
        assert data["status"] == "fail"
        assert data["log"]  # This is because of verbose=True
        assert "fail" in data["log"]


async def test_collection_validation_error(
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
):
    override_settings_factory(FRACTAL_TASKS_DIR=tmp_path)

    file_dir = tmp_path / ".fractal/fractal-tasks-mock0.0.1"
    file_path = file_dir / "collection.json"
    os.makedirs(file_dir, exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(dict(foo="bar"), f)

    payload = dict(
        package=(
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        ).as_posix()
    )

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 422
        assert "old version" in res.json()["detail"]
