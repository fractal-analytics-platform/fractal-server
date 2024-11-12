from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityActionV2
from fractal_server.tasks.v2.local import deactivate_local


async def test_deactivate_no_venv_path(tmp_path, db, first_user):
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path="/invalid",
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
        action=TaskGroupActivityActionV2.DEACTIVATE,
        pkg_name="pkg",
        version="1.0.0",
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)
    # background task
    deactivate_local(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
    )

    # Verify that deactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "does not exist" in task_group_activity_v2.log


async def test_deactivate_wheel_no_wheel_path(tmp_path, db, first_user):
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="wheel",
        wheel_path="/invalid",
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
        action=TaskGroupActivityActionV2.DEACTIVATE,
        pkg_name="pkg",
        version="1.0.0",
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)
    # background task
    deactivate_local(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
    )
    # Verify that deactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "does not exist" in task_group_activity_v2.log


async def test_collect_pip_local_fail_rmtree(
    tmp_path,
    db,
    first_user,
    current_py_version,
    monkeypatch,
):

    import fractal_server.tasks.v2.local.deactivate

    def patched_function(*args, **kwargs):
        raise RuntimeError("Broken rm")

    monkeypatch.setattr(
        fractal_server.tasks.v2.local.deactivate.shutil,
        "rmtree",
        patched_function,
    )

    # Prepare db objects
    path = tmp_path / "rmtree-error"
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-mock",
        version="0.0.1",
        origin="pypi",
        python_version=current_py_version,
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
        pip_freeze="pip",
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
    # create path and venv_path
    Path(task_group.path).mkdir()
    Path(task_group.venv_path).mkdir()
    # Run background task
    try:
        deactivate_local(
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
        )
    except RuntimeError as e:
        print(
            f"Caught exception {e} within the test, which is taking place in "
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
