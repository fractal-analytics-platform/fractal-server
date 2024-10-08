import logging
from pathlib import Path

from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import CollectionStatusV2

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
            json=dict(
                package=str(tmp_path / "missing-1.2.3-py3-none-any.whl")
            ),
        )
        assert res.status_code == 422
        assert "No such file" in str(res.json())

    # Invalid wheel file
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package=str("something.whl")),
        )
        assert res.status_code == 422


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
    THEN it returns 201 but the background task fails
    """

    override_settings_factory(FRACTAL_TASKS_DIR=tmp_path)

    # Invalid manifest
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_fail/invalid_manifest"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # API call is successful
        res = await client.post(
            f"{PREFIX}/collect/pip/", json=dict(package=wheel_path.as_posix())
        )
        assert res.status_code == 201
        collection_state_id = res.json()["id"]
        # Background task failed
        res = await client.get(f"{PREFIX}/collect/{collection_state_id}/")
        assert res.status_code == 200
        collection_data = res.json()["data"]
        assert collection_data["status"] == "fail"
        assert (
            "Manifest version manifest_version='9999' not supported"
            in collection_data["log"]
        )

    # Missing manifest
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_fail/missing_manifest"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # API call is successful
        res = await client.post(
            f"{PREFIX}/collect/pip/", json=dict(package=wheel_path.as_posix())
        )
        assert res.status_code == 201
        collection_state_id = res.json()["id"]
        # Background task failed
        res = await client.get(f"{PREFIX}/collect/{collection_state_id}/")
        assert res.status_code == 200
        collection_data = res.json()["data"]
        assert collection_data["status"] == "fail"
        assert (
            "does not include __FRACTAL_MANIFEST__.json"
            in collection_data["log"]
        )


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
        assert res.json()["data"]["status"] == CollectionStatusV2.PENDING
        state_id = res.json()["id"]
        # Inspect collection outcome
        res = await client.get(f"{PREFIX}/collect/{state_id}/?verbose=True")
        assert res.status_code == 200
        data = res.json()["data"]
        assert "missing file" in data["info"]
        assert data["status"] == "fail"
        assert data["log"]  # This is because of verbose=True
        assert "fail" in data["log"]


async def test_folder_already_exists(
    MockCurrentUser,
    client,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
):
    override_settings_factory(FRACTAL_TASKS_DIR=tmp_path)
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        # Create the folder in advance
        expected_path = tmp_path / f"{user.id}/fractal-tasks-mock/0.0.1"
        expected_path.mkdir(parents=True, exist_ok=True)
        assert expected_path.exists()

        # Fail because folder already exists
        payload = dict(
            package=(
                testdata_path.parent
                / "v2/fractal_tasks_mock/dist"
                / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
            ).as_posix()
        )
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=payload,
        )
        assert res.status_code == 422
        assert "already exists" in res.json()["detail"]

    # Failed task collection did not remove an existing folder
    assert expected_path.exists()


async def test_failure_cleanup(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
):
    """
    Verify that a failed collection cleans up its folder and TaskGroupV2.
    """

    override_settings_factory(
        FRACTAL_TASKS_DIR=tmp_path,
        FRACTAL_LOGGING_LEVEL=logging.CRITICAL,
    )

    # Valid part of the payload
    payload = dict(
        package=(
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        ).as_posix(),
        package_extras="my_extra",
    )

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        TASK_GROUP_PATH = tmp_path / str(user.id) / "fractal-tasks-mock/0.0.1"
        assert not TASK_GROUP_PATH.exists()

        # Endpoint returns correctly, despite invalid `pinned_package_versions`
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(
                **payload, pinned_package_versions={"devtools": "99.99.99"}
            ),
        )
        assert res.status_code == 201
        collection_id = res.json()["id"]

        # Background task actually failed
        res = await client.get(f"{PREFIX}/collect/{collection_id}/")
        assert (
            "No matching distribution found for devtools==99.99.99"
            in res.json()["data"]["log"]
        )

        # Cleanup was performed correctly
        assert not TASK_GROUP_PATH.exists()
        res = await db.execute(select(TaskGroupV2))
        assert len(res.scalars().all()) == 0


async def test_invalid_python_version(
    client,
    MockCurrentUser,
    override_settings_factory,
):
    override_settings_factory(
        FRACTAL_TASKS_PYTHON_3_9=None,
    )

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json=dict(package="invalid-task-package", python_version="3.9"),
        )
        assert res.status_code == 422
        assert "Python version 3.9 is not available" in res.json()["detail"]
        debug(res.json()["detail"])
