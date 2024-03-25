from pathlib import Path

from devtools import debug  # noqa

from fractal_server.tasks.endpoint_operations import inspect_package


PREFIX = "api/v2/task"


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


async def test_task_collection(
    db, client, MockCurrentUser, testdata_path: Path
):

    wheel_path = (
        testdata_path.parent
        / "v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    debug(wheel_path)

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        debug(user)
        res = await client.post(
            f"{PREFIX}/collect/pip/",
            json={"package": wheel_path.as_posix()},
        )
        assert res.status_code == 201

        assert res.json()["data"]["status"] == "pending"
        state = res.json()
        state_id = state["id"]
        data = state["data"]
        # venv_path = Path(data["venv_path"])
        assert "fractal-tasks-mock" in data["venv_path"]

        # Get/check collection info
        res = await client.get(f"{PREFIX}/collect/{state_id}/")
        assert res.status_code == 200
        state = res.json()
        data = state["data"]
        # task_list = data["task_list"]
        # task_names = (t["name"] for t in task_list)
        assert data["status"] == "OK"
        assert data["log"]
