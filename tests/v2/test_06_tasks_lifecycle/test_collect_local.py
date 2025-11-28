from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityAction
from fractal_server.tasks.v2.local import collect_local


async def test_collect_pip_existing_folder(
    tmp_path,
    db,
    first_user,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
        resource_id=resource.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.COLLECT,
        pkg_name="pkg",
        version="1.2.3",
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
        resource=resource,
        profile=profile,
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
    current_py_version,
    monkeypatch,
    local_resource_profile_db,
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
    resource, profile = local_resource_profile_db
    path = tmp_path / "rmtree-error"
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-mock",
        version="0.0.1",
        origin="local",
        archive_path=(tmp_path / "fake/path").as_posix(),
        python_version=current_py_version,
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
        resource_id=resource.id,
    )
    debug(task_group)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.COLLECT,
        pkg_name="pkg",
        version="0.0.1",
    )
    await db.commit()
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)
    # Run background task
    try:
        collect_local(
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            wheel_file=FractalUploadedFile(
                contents=b"fakebytes",
                filename="fractal_tasks_mock-0.0.1-py3-none-any.whl",
            ),
            resource=resource,
            profile=profile,
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


async def test_invalid_wheel(
    MockCurrentUser,
    tmp_path: Path,
    current_py_version,
    testdata_path: Path,
    db,
    local_resource_profile_db,
):
    """
    GIVEN a package with invalid/missing manifest or missing executable
    WHEN the 'collect_local' function is called
    THEN the expected log is shown
    """

    resource, profile = local_resource_profile_db

    pkgnames_logs = [
        ("invalid_manifest", "manifest_version"),
        ("missing_manifest", "manifest path not found"),
        ("missing_executable", "missing file"),
    ]
    async with MockCurrentUser(user_kwargs={"profile_id": profile.id}) as user:
        for name, log in pkgnames_logs:
            archive_path = (
                testdata_path.parent
                / f"v2/fractal_tasks_fail/{name}"
                / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
            )

            task_group = TaskGroupV2(
                pkg_name="fractal-tasks-mock",
                version="0.0.1",
                origin="local",
                archive_path=archive_path.as_posix(),
                python_version=current_py_version,
                path=(tmp_path / name).as_posix(),
                venv_path=(tmp_path / name / "venv").as_posix(),
                user_id=user.id,
                resource_id=resource.id,
            )

            db.add(task_group)
            await db.commit()
            await db.refresh(task_group)
            db.expunge(task_group)
            task_group_activity = TaskGroupActivityV2(
                user_id=user.id,
                taskgroupv2_id=task_group.id,
                status=TaskGroupActivityStatus.PENDING,
                action=TaskGroupActivityAction.COLLECT,
                pkg_name="pkg",
                version="1.0.0",
            )
            await db.commit()
            db.add(task_group_activity)
            await db.commit()
            await db.refresh(task_group_activity)
            db.expunge(task_group_activity)

            with open(archive_path, "rb") as whl:
                collect_local(
                    task_group_id=task_group.id,
                    task_group_activity_id=task_group_activity.id,
                    wheel_file=FractalUploadedFile(
                        contents=whl.read(),
                        filename=archive_path.name,
                    ),
                    resource=resource,
                    profile=profile,
                )

            task_group_activity = await db.get(
                TaskGroupActivityV2, task_group_activity.id
            )
            assert task_group_activity.status == (
                TaskGroupActivityStatus.FAILED
            )
            assert task_group_activity.timestamp_ended is not None
            assert log in task_group_activity.log
