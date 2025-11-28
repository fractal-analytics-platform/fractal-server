from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityAction
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2.ssh import reactivate_ssh


def _reset_permissions(remote_folder: str, fractal_ssh: FractalSSH):
    """
    This is useful to avoid "garbage" folders (in pytest tmp folder) that
    cannot be removed because of wrong permissions.
    """
    import logging

    logging.warning(f"[_reset_permissions] {remote_folder=}")
    if fractal_ssh.remote_exists(remote_folder):
        fractal_ssh.run_command(cmd=f"chmod -R 777 {remote_folder}")


@pytest.mark.container
async def test_reactivate_ssh_venv_exists(
    tmp777_path,
    db,
    first_user,
    fractal_ssh: FractalSSH,
    slurm_ssh_resource_profile_db,
):
    resource, profile = slurm_ssh_resource_profile_db

    path = Path(profile.tasks_remote_dir) / "package"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=f"{tmp777_path}/i_am_here",
        user_id=first_user.id,
        resource_id=resource.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.DEACTIVATE,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge_all()

    # create venv_path
    fractal_ssh.mkdir(folder=task_group.venv_path)

    # background task
    reactivate_ssh(
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

    _reset_permissions(
        fractal_ssh=fractal_ssh,
        remote_folder=profile.tasks_remote_dir,
    )


@pytest.mark.parametrize("make_rmtree_fail", [False, True])
@pytest.mark.container
async def test_reactivate_ssh_fail(
    db,
    first_user,
    monkeypatch,
    make_rmtree_fail: bool,
    fractal_ssh: FractalSSH,
    current_py_version,
    slurm_ssh_resource_profile_db,
):
    """
    Make reactivation fail (due to wrong pip-freeze data), in two cases:
    1. The removal of the venv path works.
    2. The removal of the venv path fails.
    """

    if make_rmtree_fail:
        import fractal_server.tasks.v2.ssh._utils

        FAILED_RMTREE_MESSAGE = "Broken rm"

        def patched_rmtree(*args, **kwargs):
            raise RuntimeError(FAILED_RMTREE_MESSAGE)

        monkeypatch.setattr(
            fractal_server.tasks.v2.ssh._utils.FractalSSH,
            "remove_folder",
            patched_rmtree,
        )

    resource, profile = slurm_ssh_resource_profile_db

    # Prepare task group that will make `pip install` fail
    path = (
        Path(profile.tasks_remote_dir) / f"make-rmtree-fail-{make_rmtree_fail}"
    )
    task_group = TaskGroupV2(
        pkg_name="invalid-package-name",
        version="11.11.11",
        origin="pypi",
        python_version=current_py_version,
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
        env_info="something==99.99.99",
        resource_id=resource.id,
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
    fractal_ssh.mkdir(folder=task_group.path)

    # Run background task
    try:
        reactivate_ssh(
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

    MSG = "No matching distribution found for something==99.99.99"
    assert MSG in activity.log
    assert Path(task_group.path).exists()

    if make_rmtree_fail:
        assert FAILED_RMTREE_MESSAGE in activity.log
        assert fractal_ssh.remote_exists(task_group.venv_path)
    else:
        assert not fractal_ssh.remote_exists(task_group.venv_path)

    _reset_permissions(
        fractal_ssh=fractal_ssh,
        remote_folder=profile.tasks_remote_dir,
    )
