import json
import logging
from pathlib import Path

from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import WheelFile
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityActionV2
from fractal_server.tasks.v2.local import collect_local


async def test_collect_pip_existing_file(tmp_path, db, first_user):
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.COLLECT,
        pkg_name="pkg",
        version="1.0.0",
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)
    # Create task_group.path
    path.mkdir()
    # Run background task
    collect_local(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        wheel_file=None,
    )
    # Verify that collection failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert task_group_activity_v2.taskgroupv2_id is None


async def test_collect_pip_local_fail_rmtree(
    tmp_path,
    db,
    first_user,
    testdata_path,
    current_py_version,
    monkeypatch,
):
    import fractal_server.tasks.v2.local.collect

    def patched_function(*args, **kwargs):
        raise RuntimeError("Broken rm")

    monkeypatch.setattr(
        fractal_server.tasks.v2.local.collect.shutil,
        "rmtree",
        patched_function,
    )

    # Prepare db objects
    path = tmp_path / "rmtree-error"
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-mock",
        version="0.0.1",
        origin="local",
        wheel_path=(
            testdata_path.parent
            / (
                "v2/fractal_tasks_fail/invalid_manifest/dist/"
                "fractal_tasks_mock-0.0.1-py3-none-any.whl"
            )
        ).as_posix(),
        python_version=current_py_version,
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
    )
    debug(task_group)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.COLLECT,
        pkg_name="pkg",
        version="1.0.0",
    )
    await db.commit()
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)
    # Run background task
    wheel_path = testdata_path.parent / (
        "v2/fractal_tasks_fail/invalid_manifest/dist/"
        "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(wheel_path, "rb") as whl:
        try:
            collect_local(
                task_group_id=task_group.id,
                task_group_activity_id=task_group_activity.id,
                wheel_file=WheelFile(
                    contents=whl.read(),
                    filename=wheel_path.name,
                ),
            )
        except RuntimeError as e:
            print(
                f"Caught exception {e} within the test, "
                "which is taking place in "
                "the `rmtree` call that cleans up `tmpdir`. Safe to ignore."
            )
        # Verify that collection failed
        task_group_activity_v2 = await db.get(
            TaskGroupActivityV2, task_group_activity.id
        )
        debug(task_group_activity_v2)
        assert task_group_activity_v2.status == "failed"
        assert "Broken rm" in task_group_activity_v2.log
        assert path.exists()


async def test_bad_wheel_file_arguments(
    tmp_path,
    db,
    first_user,
    testdata_path,
    current_py_version,
    monkeypatch,
):
    import fractal_server.tasks.v2.local.collect

    def patched_function(*args, **kwargs):
        raise RuntimeError("Broken rm")

    monkeypatch.setattr(
        fractal_server.tasks.v2.local.collect.shutil,
        "rmtree",
        patched_function,
    )

    # Prepare db objects
    path = tmp_path / "rmtree-error"
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-mock",
        version="0.0.1",
        origin="local",
        wheel_path=(
            testdata_path.parent
            / (
                "v2/fractal_tasks_fail/invalid_manifest/dist/"
                "fractal_tasks_mock-0.0.1-py3-none-any.whl"
            )
        ).as_posix(),
        python_version=current_py_version,
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
    )
    debug(task_group)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.COLLECT,
        pkg_name="pkg",
        version="1.0.0",
    )
    await db.commit()
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)
    # Run background task
    wheel_path = testdata_path.parent / (
        "v2/fractal_tasks_fail/invalid_manifest/dist/"
        "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )
    with open(wheel_path, "rb") as whl:
        try:
            collect_local(
                task_group_id=task_group.id,
                task_group_activity_id=task_group_activity.id,
                wheel_file=WheelFile(
                    contents=whl.read(),
                    filename=wheel_path.name,
                ),
            )
        except RuntimeError as e:
            print(
                f"Caught exception {e} within the test, "
                "which is taking place in "
                "the `rmtree` call that cleans up `tmpdir`. Safe to ignore."
            )
        # Verify that collection failed
        task_group_activity_v2 = await db.get(
            TaskGroupActivityV2, task_group_activity.id
        )
        debug(task_group_activity_v2)
        assert task_group_activity_v2.status == "failed"
        assert "Broken rm" in task_group_activity_v2.log
        assert path.exists()


async def test_invalid_manifest(
    client,
    MockCurrentUser,
    override_settings_factory,
    tmp_path: Path,
    current_py_version,
    testdata_path: Path,
    db,
):
    """
    GIVEN a package with invalid/missing manifest
    WHEN the 'collect_local' function is called
    THEN the expected log is shown
    """

    override_settings_factory(FRACTAL_TASKS_DIR=tmp_path)

    pkgnames_logs = [
        ("invalid_manifest", "Wrong manifest version"),
        ("missing_manifest", "manifest path not found"),
    ]
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        for name, log in pkgnames_logs:
            wheel_path = (
                testdata_path.parent
                / f"v2/fractal_tasks_fail/{name}"
                / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
            )

            task_group = TaskGroupV2(
                pkg_name="fractal-tasks-mock",
                version="0.0.1",
                origin="local",
                wheel_path=wheel_path,
                python_version=current_py_version,
                path=(tmp_path / name).as_posix(),
                venv_path=(tmp_path / name / "venv").as_posix(),
                user_id=user.id,
            )

            db.add(task_group)
            await db.commit()
            await db.refresh(task_group)
            db.expunge(task_group)
            task_group_activity = TaskGroupActivityV2(
                user_id=user.id,
                taskgroupv2_id=task_group.id,
                status=TaskGroupActivityStatusV2.PENDING,
                action=TaskGroupActivityActionV2.COLLECT,
                pkg_name="pkg",
                version="1.0.0",
            )
            await db.commit()
            db.add(task_group_activity)
            await db.commit()
            await db.refresh(task_group_activity)
            db.expunge(task_group_activity)

            with open(wheel_path, "rb") as whl:
                collect_local(
                    task_group_id=task_group.id,
                    task_group_activity_id=task_group_activity.id,
                    wheel_file=WheelFile(
                        contents=whl.read(),
                        filename=wheel_path.name,
                    ),
                )

            res = await client.get(
                f"/api/v2/task-group/activity/{task_group_activity.id}/"
            )
            task_group_activity = res.json()
            debug(task_group_activity)
            assert task_group_activity["status"] == "failed"
            assert task_group_activity["timestamp_ended"] is not None
            assert log in task_group_activity["log"]


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
        files = {"file": (wheel_path.name, f.read(), "application/zip")}
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)):
        # Trigger collection
        res = await client.post(
            "api/v2/task/collect/pip/",
            data={},
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
    payload = dict(package_extras="my_extra")

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        wheel_path = (
            testdata_path.parent
            / "v2/fractal_tasks_mock/dist"
            / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
        with open(wheel_path, "rb") as f:
            files = {"file": (wheel_path.name, f.read(), "application/zip")}
        TASK_GROUP_PATH = tmp_path / str(user.id) / "fractal-tasks-mock/0.0.1"
        assert not TASK_GROUP_PATH.exists()

        # Endpoint returns correctly,
        # despite invalid `pinned_package_versions`
        res = await client.post(
            "api/v2/task/collect/pip/",
            data=dict(
                **payload,
                pinned_package_versions=json.dumps({"pydantic": "99.99.99"}),
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
