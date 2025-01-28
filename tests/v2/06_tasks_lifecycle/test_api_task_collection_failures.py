from pathlib import Path

from devtools import debug

PREFIX = "api/v2/task"


async def test_non_verified_user(client, MockCurrentUser, testdata_path):
    # Task collection triggered by non-verified user
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(wheel_path, "rb") as f:
        files = {"file": (wheel_path.name, f.read(), "application/zip")}
    # Task collection triggered by non-verified user
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            data={},
            files=files,
        )
        assert res.status_code == 401


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
            files = {"file": (wheel_path.name, f.read(), "application/zip")}
        # Fail because folder already exists
        res = await client.post(f"{PREFIX}/collect/pip/", data={}, files=files)
        assert res.status_code == 422
        assert "already exists" in res.json()["detail"]

    # Failed task collection did not remove an existing folder
    assert expected_path.exists()


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


async def test_wheel_collection_failures(
    client,
    MockCurrentUser,
    testdata_path: Path,
):
    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(wheel_path, "rb") as f:
        wheel_file_content = f.read()

    files = {"file": (wheel_path.name, wheel_file_content, "application/zip")}

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
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
