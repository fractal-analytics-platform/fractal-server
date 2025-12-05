import json
import logging
from pathlib import Path

from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v2 import TaskGroupV2

PREFIX = "api/v2/task"


async def test_(client, MockCurrentUser, testdata_path):
    # Task collection triggered by non-verified user
    archive_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(archive_path, "rb") as f:
        files = {"file": (archive_path.name, f.read(), "application/zip")}
    # Task collection triggered by non-verified user
    async with MockCurrentUser(is_verified=False):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data={},
            files=files,
        )
        assert res.status_code == 401


async def test_folder_already_exists(
    MockCurrentUser,
    client,
    testdata_path: Path,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(is_verified=True, profile_id=profile.id) as user:
        # Create the folder in advance
        expected_path = (
            Path(resource.tasks_local_dir)
            / f"{user.id}/fractal-tasks-mock/0.0.1"
        )
        expected_path.mkdir(parents=True, exist_ok=True)
        assert expected_path.exists()
        archive_path = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
        with open(archive_path, "rb") as f:
            files = {"file": (archive_path.name, f.read(), "application/zip")}
        # Fail because folder already exists
        res = await client.post(f"{PREFIX}/collect/pip/", data={}, files=files)
        assert res.status_code == 422
        assert "already exists" in res.json()["detail"]

    # Failed task collection did not remove an existing folder
    assert expected_path.exists()


async def test_invalid_python_version(
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(package="invalid-task-package", python_version="3.9"),
        )
        assert res.status_code == 422
        assert "Python version 3.9 is not available" in res.json()["detail"]
        debug(res.json()["detail"])


async def test_wheel_collection_failures(
    client,
    MockCurrentUser,
    testdata_path: Path,
    local_resource_profile_db,
):
    archive_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(archive_path, "rb") as f:
        wheel_file_content = f.read()

    files = {"file": (archive_path.name, wheel_file_content, "application/zip")}

    resource, profile = local_resource_profile_db
    async with MockCurrentUser(is_verified=True, profile_id=profile.id):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data={},
            files={},
        )
        assert res.status_code == 422
        MSG = "When no `file` is provided, `package` is required."
        assert MSG in res.json()["detail"]

        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(package="fractal_tasks_mock"),
            files=files,
        )
        assert res.status_code == 422
        MSG = "Cannot set `package` when `file` is provided"
        assert MSG in res.json()["detail"]

        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data=dict(package_version="0.0.1"),
            files=files,
        )
        assert res.status_code == 422
        MSG = "Cannot set `package_version` when `file` is provided"
        assert MSG in res.json()["detail"]

        files = {"file": ("something", wheel_file_content, "application/zip")}
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "Invalid wheel-file name" in str(res.json()["detail"])
        debug(res.json())

        files = {
            "file": ("something.whl", wheel_file_content, "application/zip")
        }
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "Invalid wheel-file name" in str(res.json()["detail"])
        debug(res.json())

        files = {
            "file": (
                "something;rm /invalid/path.whl",
                wheel_file_content,
                "application/zip",
            )
        }
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data={},
            files=files,
        )
        assert res.status_code == 422
        assert "Wheel filename has forbidden characters" in str(
            res.json()["detail"]
        )
        debug(res.json())


async def test_failure_cleanup(
    db,
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    testdata_path: Path,
    local_resource_profile_db,
):
    """
    Verify that a failed collection cleans up its folder and TaskGroupV2.
    """

    override_settings_factory(FRACTAL_LOGGING_LEVEL=logging.CRITICAL)

    resource, profile = local_resource_profile_db

    # Valid part of the payload
    payload = dict(package_extras="my_extra")

    async with MockCurrentUser(is_verified=True, profile_id=profile.id) as user:
        archive_path = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
        with open(archive_path, "rb") as f:
            files = {"file": (archive_path.name, f.read(), "application/zip")}
        TASK_GROUP_PATH = tmp_path / str(user.id) / "fractal-tasks-mock/0.0.1"
        assert not TASK_GROUP_PATH.exists()

        # Endpoint returns correctly, despite invalid
        # `pinned_package_versions_pre` and `pinned_package_versions_post`
        res = await client.post(
            "api/v2/task/collect/pip/",
            data=dict(
                **payload,
                pinned_package_versions_pre=json.dumps(
                    {"pydantic": "99.99.99"}
                ),
                pinned_package_versions_post=json.dumps(
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
