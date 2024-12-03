import json
import logging
from pathlib import Path

from devtools import debug  # noqa
from sqlmodel import select

from fractal_server.app.models.v2 import TaskGroupV2

PREFIX = "api/v2/task"


async def test_failed_API_calls(
    client, MockCurrentUser, tmp_path, testdata_path
):
    # Task collection triggered by non-verified user
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(wheel_path, "rb") as f:
        files = {"file": (wheel_path.name, f, "application/zip")}
        # Task collection triggered by non-verified user
        async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
            res = await client.post(
                f"{PREFIX}/collect/pip/",
                data={"package": "fractal-tasks-core"},
                files=files,
            )
            assert res.status_code == 401

        # Missing wheel file
        async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
            res = await client.post(
                f"{PREFIX}/collect/pip/",
                data=dict(
                    package=str(tmp_path / "missing-1.2.3-py3-none-any.whl")
                ),
            )
            assert res.status_code == 422
            assert "Missing valid wheel-file" in str(res.json())

        # Non-absolute wheel file
        async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
            res = await client.post(
                f"{PREFIX}/collect/pip/",
                data=dict(package=str("something.whl")),
                files=files,
            )
            assert res.status_code == 422
            assert "must be absolute" in str(res.json()["detail"])


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
    THEN it returns 202 but the background task fails
    """

    override_settings_factory(FRACTAL_TASKS_DIR=tmp_path)

    # Invalid manifest
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_fail/invalid_manifest"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(wheel_path, "rb") as f:
        files = {"file": (wheel_path.name, f, "application/zip")}
        async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
            # API call is successful
            res = await client.post(
                f"{PREFIX}/collect/pip/",
                data=dict(package=wheel_path.as_posix()),
                files=files,
            )

            assert res.status_code == 202
            task_group_activity_id = res.json()["id"]
            # Background task failed
            res = await client.get(
                f"/api/v2/task-group/activity/{task_group_activity_id}/"
            )
            task_group_activity = res.json()
            assert task_group_activity["status"] == "failed"
            assert task_group_activity["timestamp_ended"] is not None
            assert "Wrong manifest version" in task_group_activity["log"]

    # Missing manifest
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_fail/missing_manifest"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(wheel_path, "rb") as f:
        files = {"file": (wheel_path.name, f, "application/zip")}
        async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
            # API call is successful
            res = await client.post(
                f"{PREFIX}/collect/pip/",
                data=dict(package=wheel_path.as_posix()),
                files=files,
            )

            assert res.status_code == 202
            task_group_activity_id = res.json()["id"]
            # Background task failed
            res = await client.get(
                f"/api/v2/task-group/activity/{task_group_activity_id}/"
            )
            task_group_activity = res.json()
            assert task_group_activity["status"] == "failed"
            assert task_group_activity["timestamp_ended"] is not None
            assert "manifest path not found" in task_group_activity["log"]


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
    with open(wheel_path, "rb") as f:
        files = {"file": (wheel_path.name, f, "application/zip")}
        async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
            # Trigger collection
            res = await client.post(
                f"{PREFIX}/collect/pip/",
                data=dict(package=wheel_path.as_posix()),
                files=files,
            )

            assert res.status_code == 202
            assert res.json()["status"] == "pending"

            task_group_activity_id = res.json()["id"]
            # Background task failed
            res = await client.get(
                f"/api/v2/task-group/activity/{task_group_activity_id}/"
            )
            assert res.status_code == 200
            task_group_activity = res.json()
            assert task_group_activity["status"] == "failed"
            assert task_group_activity["timestamp_ended"] is not None
            assert "missing file" in task_group_activity["log"]


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
        wheel_path = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
        with open(wheel_path, "rb") as f:
            files = {"file": (wheel_path.name, f, "application/zip")}
            # Fail because folder already exists
            payload = dict(
                package=(
                    testdata_path.parent
                    / "v2/fractal_tasks_mock/dist"
                    / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
                ).as_posix(),
            )
            res = await client.post(
                f"{PREFIX}/collect/pip/", data=payload, files=files
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
        wheel_path = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
        with open(wheel_path, "rb") as f:
            files = {"file": (wheel_path.name, f, "application/zip")}
            TASK_GROUP_PATH = (
                tmp_path / str(user.id) / "fractal-tasks-mock/0.0.1"
            )
            assert not TASK_GROUP_PATH.exists()

            # Endpoint returns correctly,
            # despite invalid `pinned_package_versions`
            res = await client.post(
                f"{PREFIX}/collect/pip/",
                data=dict(
                    **payload,
                    pinned_package_versions=json.dumps(
                        {"pydantic": "99.99.99"}
                    ),
                ),
                files=files,
            )
            assert res.status_code == 202
            task_group_activity_id = res.json()["id"]
            # Background task failed
            res = await client.get(
                f"/api/v2/task-group/activity/{task_group_activity_id}/"
            )
            task_group_activity = res.json()
            assert task_group_activity["status"] == "failed"
            assert task_group_activity["timestamp_ended"] is not None
            assert (
                "No matching distribution found for pydantic==99.99.99"
                in task_group_activity["log"]
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
            data=dict(package="invalid-task-package", python_version="3.9"),
        )
        assert res.status_code == 422
        assert "Python version 3.9 is not available" in res.json()["detail"]
        debug(res.json()["detail"])
