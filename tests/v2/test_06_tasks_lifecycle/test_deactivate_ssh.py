from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2 import TaskGroupOriginEnum
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityAction
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2.ssh import collect_ssh
from fractal_server.tasks.v2.ssh import deactivate_ssh


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
async def test_deactivate_fail_no_venv_path(
    tmp777_path,
    db,
    first_user,
    slurm_ssh_resource_profile_db,
    fractal_ssh,
):
    resource, profile = slurm_ssh_resource_profile_db
    path = tmp777_path / "something"
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
        action=TaskGroupActivityAction.DEACTIVATE,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)

    # background task
    deactivate_ssh(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
    )

    # Verify that deactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "does not exist" in task_group_activity_v2.log

    _reset_permissions(
        fractal_ssh=fractal_ssh,
        remote_folder=profile.tasks_remote_dir,
    )


@pytest.mark.container
async def test_deactivate_ssh_fail(
    tmp777_path,
    db,
    first_user,
    monkeypatch,
    fractal_ssh,
    slurm_ssh_resource_profile_db,
):
    FAKE_ERROR_MSG = "this is some fake error message"

    resource, profile = slurm_ssh_resource_profile_db

    def fail_function(*args, **kwargs):
        raise RuntimeError(FAKE_ERROR_MSG)

    import fractal_server.tasks.v2.ssh.deactivate

    monkeypatch.setattr(
        fractal_server.tasks.v2.ssh.deactivate,
        "_customize_and_run_template",
        fail_function,
    )

    path = Path(profile.tasks_remote_dir) / "something"
    venv_path = path / "venv"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=venv_path.as_posix(),
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

    fractal_ssh.mkdir(folder=path)
    fractal_ssh.mkdir(folder=venv_path)

    # background task
    deactivate_ssh(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
    )

    # Verify that deactivate failed
    activity = await db.get(TaskGroupActivityV2, task_group_activity.id)
    assert activity.status == "failed"
    assert FAKE_ERROR_MSG in activity.log

    _reset_permissions(
        fractal_ssh=fractal_ssh,
        remote_folder=profile.tasks_remote_dir,
    )


@pytest.mark.container
async def test_deactivate_wheel_no_archive_path(
    db,
    first_user,
    fractal_ssh,
    slurm_ssh_resource_profile_db,
):
    resource, profile = slurm_ssh_resource_profile_db
    # Prepare db objects
    path = Path(profile.tasks_remote_dir) / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin=TaskGroupOriginEnum.WHEELFILE,
        archive_path="/invalid",
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
        env_info="pip",
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
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)
    db.expunge(task_group_activity)
    # create path and venv_path
    fractal_ssh.mkdir(folder=task_group.path)
    fractal_ssh.mkdir(folder=task_group.venv_path)

    # background task
    deactivate_ssh(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
    )
    # Verify that deactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "does not exist" in task_group_activity_v2.log
    assert "Invalid wheel path" in task_group_activity_v2.log

    _reset_permissions(
        fractal_ssh=fractal_ssh,
        remote_folder=profile.tasks_remote_dir,
    )


@pytest.mark.container
async def test_deactivate_wheel_package_created_before_2_9_0(
    db,
    first_user,
    current_py_version,
    testdata_path,
    fractal_ssh,
    slurm_ssh_resource_profile_db,
):
    resource, profile = slurm_ssh_resource_profile_db

    # STEP 1: collect a package
    path = Path(profile.tasks_remote_dir) / "fractal-tasks-mock-path"
    venv_path = path / "venv"
    local_archive_path = (
        testdata_path.parent
        / (
            "v2/fractal_tasks_mock/dist/"
            "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
    ).as_posix()
    archive_path = (
        path / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()
    with open(local_archive_path, "rb") as wheel_file:
        wheel_buffer = wheel_file.read()
    task_group = TaskGroupV2(
        pkg_name="fractal_tasks_mock",
        version="0.0.1",
        origin=TaskGroupOriginEnum.WHEELFILE,
        archive_path=archive_path,
        path=path.as_posix(),
        venv_path=venv_path.as_posix(),
        user_id=first_user.id,
        env_info="pip",
        python_version=current_py_version,
        resource_id=resource.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    activity_collect = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.COLLECT,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(activity_collect)
    await db.commit()
    await db.refresh(activity_collect)
    db.expunge_all()

    collect_ssh(
        task_group_id=task_group.id,
        task_group_activity_id=activity_collect.id,
        wheel_file=FractalUploadedFile(
            contents=wheel_buffer,
            filename=Path(archive_path).name,
        ),
        resource=resource,
        profile=profile,
    )
    activity_collect = await db.get(TaskGroupActivityV2, activity_collect.id)
    assert activity_collect.status == TaskGroupActivityStatus.OK

    # STEP 2: make it look like a pre-2.9.0 package, both in the db and
    # in the virtual environment
    task_group = await db.get(TaskGroupV2, task_group.id)
    task_group.env_info = None
    task_group.archive_path = archive_path
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    python_bin = (venv_path / "bin/python").as_posix()
    pip_install_new_wheel = (
        f"{python_bin} -m pip install {archive_path} --force-reinstall"
    )
    fractal_ssh.run_command(cmd=pip_install_new_wheel)

    # STEP 3: Deactivate the task group
    activity_deactivate = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.DEACTIVATE,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
    )
    db.add(activity_deactivate)
    await db.commit()
    await db.refresh(activity_deactivate)
    db.expunge_all()

    deactivate_ssh(
        task_group_id=task_group.id,
        task_group_activity_id=activity_deactivate.id,
        resource=resource,
        profile=profile,
    )

    # Check outcome
    activity_deactivate = await db.get(
        TaskGroupActivityV2, activity_deactivate.id
    )
    task_group = await db.get(TaskGroupV2, task_group.id)
    assert activity_deactivate.status == TaskGroupActivityStatus.OK
    print(activity_deactivate.log)
    assert "Recreate pip-freeze information" in activity_deactivate.log

    _reset_permissions(
        fractal_ssh=fractal_ssh,
        remote_folder=profile.tasks_remote_dir,
    )


@pytest.mark.container
async def test_deactivate_ssh_github_dependency(
    tmp777_path,
    db,
    first_user,
    fractal_ssh,
    slurm_ssh_resource_profile_db,
):
    resource, profile = slurm_ssh_resource_profile_db
    path = Path(profile.tasks_remote_dir) / "something"
    venv_path = path / "venv"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=venv_path.as_posix(),
        user_id=first_user.id,
        env_info=(
            "BaSiCPy @ "
            "git+https://github.com/"
            "peng-lab/BaSiCPy.git"
            "@166bf6190c1827b5a5ece4a5542433c96a2bc997"
            "\n"
        ),
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

    fractal_ssh.mkdir(folder=path)
    fractal_ssh.mkdir(folder=venv_path)

    # background task
    deactivate_ssh(
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
    )

    # Verify that deactivate failed
    activity = await db.get(TaskGroupActivityV2, task_group_activity.id)
    assert activity.status == "failed"
    assert "github.com" in activity.log
    assert "not currently supported" in activity.log

    _reset_permissions(
        fractal_ssh=fractal_ssh,
        remote_folder=profile.tasks_remote_dir,
    )
