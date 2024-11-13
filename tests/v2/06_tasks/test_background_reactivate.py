from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityActionV2
from fractal_server.tasks.v2.local import reactivate_local


def test_unit_missing_objects(db, caplog):
    """
    Test a branch which is in principle unreachable.
    """
    caplog.clear()
    reactivate_local(
        task_group_activity_id=9999,
        task_group_id=9999,
    )
    assert "Cannot find database rows" in caplog.text

    caplog.clear()
    assert caplog.text == ""

    # not implemented
    # collect_package_ssh(
    #     task_group_activity_id=9999,
    #     task_group_id=9999,
    #     fractal_ssh=None,
    #     tasks_base_dir="/invalid",
    # )
    # assert "Cannot find database rows" in caplog.text


async def test_reactivate_venv_path(tmp_path, db, first_user):
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=f"{tmp_path}/i_am_here",
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
    # create venv_path
    Path(task_group.venv_path).mkdir()
    # background task
    reactivate_local(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
    )

    # Verify that reactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "already exists" in task_group_activity_v2.log


@pytest.mark.parametrize("make_rmtree_fail", [True, False])
async def test_reactivate_local_fail(
    tmp_path,
    db,
    first_user,
    current_py_version,
    monkeypatch,
    make_rmtree_fail: bool,
):
    """
    Make reactivation fail (due to wrong pip-freeze data), in two cases:
    1. The removal of the venv path works.
    2. The removal of the venv path fails.
    """

    if make_rmtree_fail:
        import fractal_server.tasks.v2.local.reactivate

        FAILED_RMTREE_MESSAGE = "Broken rm"

        def patched_rmtree(*args, **kwargs):
            raise RuntimeError(FAILED_RMTREE_MESSAGE)

        monkeypatch.setattr(
            fractal_server.tasks.v2.local.reactivate.shutil,
            "rmtree",
            patched_rmtree,
        )

    # Prepare task group that will make `pip install` fail
    path = tmp_path / f"make-rmtree-fail-{make_rmtree_fail}"
    task_group = TaskGroupV2(
        pkg_name="invalid-package-name",
        version="11.11.11",
        origin="pypi",
        python_version=current_py_version,
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
        pip_freeze="something==99.99.99",
    )
    debug(task_group)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.REACTIVATE,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge_all()

    # Create path
    Path(task_group.path).mkdir()

    # Run background task
    reactivate_local(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
    )

    # Verify that collection failed
    activity = await db.get(TaskGroupActivityV2, task_group_activity.id)
    debug(activity.status)
    debug(activity.log)
    assert activity.status == "failed"
    MSG = "Could not find a version that satisfies the requirement something==99.99.99"  # noqa
    assert MSG in activity.log
    assert Path(task_group.path).exists()

    if make_rmtree_fail:
        assert FAILED_RMTREE_MESSAGE in activity.log
        assert Path(task_group.venv_path).exists()
    else:
        assert not Path(task_group.venv_path).exists()
