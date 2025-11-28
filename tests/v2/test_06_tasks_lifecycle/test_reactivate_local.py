import logging
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityAction
from fractal_server.tasks.v2.local import reactivate_local


async def test_reactivate_local_venv_exists(
    tmp_path, db, first_user, local_resource_profile_db
):
    # Prepare db objects
    resource, profile = local_resource_profile_db
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=f"{tmp_path}/i_am_here",
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
        action=TaskGroupActivityAction.DEACTIVATE,
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
        resource=resource,
        profile=profile,
    )

    # Verify that reactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "already exists" in task_group_activity_v2.log


@pytest.mark.parametrize("make_rmtree_fail", [False, True])
async def test_reactivate_local_fail(
    tmp_path,
    db,
    first_user,
    current_py_version,
    monkeypatch,
    make_rmtree_fail: bool,
    local_resource_profile_db,
):
    """
    Make reactivation fail (due to wrong pip-freeze data), in two cases:
    1. The removal of the venv path works.
    2. The removal of the venv path fails.
    """

    import fractal_server.tasks.v2.local.reactivate

    FAILED_RMTREE_MESSAGE = "Broken rm"

    def patched_rmtree(*args, **kwargs):
        if make_rmtree_fail:
            raise RuntimeError(FAILED_RMTREE_MESSAGE)
        else:
            logging.warning("Mock of `shutil.rmtree`.")

    monkeypatch.setattr(
        fractal_server.tasks.v2.local.reactivate.shutil,
        "rmtree",
        patched_rmtree,
    )

    # Prepare task group that will make `pip install` fail
    resource, profile = local_resource_profile_db
    path = tmp_path / f"make-rmtree-fail-{make_rmtree_fail}"
    task_group = TaskGroupV2(
        pkg_name="invalid-package-name",
        version="11.11.11",
        origin="pypi",
        python_version=current_py_version,
        path=path.as_posix(),
        venv_path="/fake/folder/impossible/",
        user_id=first_user.id,
        resource_id=resource.id,
        env_info="something==99.99.99",
    )
    debug(task_group)
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.REACTIVATE,
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
    try:
        reactivate_local(
            task_group_id=task_group.id,
            task_group_activity_id=task_group_activity.id,
            resource=resource,
            profile=profile,
        )
    except RuntimeError as e:
        print(
            f"Caught exception {e} within the test, which is taking place in "
            "the `rmtree` call that cleans up `tmpdir`. Safe to ignore."
        )

    # Verify that collection failed
    activity = await db.get(TaskGroupActivityV2, task_group_activity.id)
    debug(activity.status)
    debug(activity.log)
    assert activity.status == "failed"

    if make_rmtree_fail:
        assert FAILED_RMTREE_MESSAGE in activity.log
