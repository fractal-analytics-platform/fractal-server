from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.schemas.v2 import TaskGroupActivityStatusV2
from fractal_server.app.schemas.v2 import TaskGroupV2OriginEnum
from fractal_server.app.schemas.v2.task_group import TaskGroupActivityActionV2
from fractal_server.tasks.v2.ssh import collect_ssh
from fractal_server.tasks.v2.ssh import deactivate_ssh


async def test_deactivate_fail_no_venv_path(
    tmp777_path,
    db,
    first_user,
    fractal_ssh,
):
    path = tmp777_path / "something"
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
        action=TaskGroupActivityActionV2.DEACTIVATE,
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
        fractal_ssh=fractal_ssh,
        tasks_base_dir=tmp777_path.as_posix(),
    )

    # Verify that deactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "does not exist" in task_group_activity_v2.log


async def test_deactivate_ssh_fail(
    tmp777_path,
    db,
    first_user,
    monkeypatch,
    fractal_ssh,
):
    FAKE_ERROR_MSG = "this is some fake error message"

    def fail_function(*args, **kwargs):
        raise RuntimeError(FAKE_ERROR_MSG)

    import fractal_server.tasks.v2.ssh.deactivate

    monkeypatch.setattr(
        fractal_server.tasks.v2.ssh.deactivate,
        "_customize_and_run_template",
        fail_function,
    )

    path = tmp777_path / "something"
    venv_path = path / "venv"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin="pypi",
        path=path.as_posix(),
        venv_path=venv_path.as_posix(),
        user_id=first_user.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    task_group_activity = TaskGroupActivityV2(
        user_id=first_user.id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatusV2.PENDING,
        action=TaskGroupActivityActionV2.DEACTIVATE,
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
        fractal_ssh=fractal_ssh,
        tasks_base_dir=tmp777_path.as_posix(),
    )

    # Verify that deactivate failed
    activity = await db.get(TaskGroupActivityV2, task_group_activity.id)
    assert activity.status == "failed"
    assert FAKE_ERROR_MSG in activity.log


async def test_deactivate_wheel_no_wheel_path(
    tmp777_path,
    db,
    first_user,
    fractal_ssh,
):
    # Prepare db objects
    path = tmp777_path / "something"
    task_group = TaskGroupV2(
        pkg_name="pkg",
        version="1.2.3",
        origin=TaskGroupV2OriginEnum.WHEELFILE,
        wheel_path="/invalid",
        path=path.as_posix(),
        venv_path=(path / "venv").as_posix(),
        user_id=first_user.id,
        pip_freeze="pip",
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
        fractal_ssh=fractal_ssh,
        tasks_base_dir=tmp777_path.as_posix(),
    )
    # Verify that deactivate failed
    task_group_activity_v2 = await db.get(
        TaskGroupActivityV2, task_group_activity.id
    )
    debug(task_group_activity_v2)
    assert task_group_activity_v2.status == "failed"
    assert "does not exist" in task_group_activity_v2.log
    assert "Invalid wheel path" in task_group_activity_v2.log


async def test_deactivate_wheel_package_created_before_2_9_0(
    db,
    first_user,
    current_py_version,
    testdata_path,
    fractal_ssh,
    tmp777_path,
    override_settings_factory,
):
    # Setup remote Python interpreter
    current_py_version_underscore = current_py_version.replace(".", "_")
    key = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    value = f"/.venv{current_py_version}/bin/python{current_py_version}"
    override_settings_factory(**{key: value})

    # STEP 1: collect a package
    path = tmp777_path / "fractal-tasks-mock-path"
    venv_path = path / "venv"
    local_wheel_path = (
        testdata_path.parent
        / (
            "v2/fractal_tasks_mock/dist/"
            "fractal_tasks_mock-0.0.1-py3-none-any.whl"
        )
    ).as_posix()
    wheel_path = (
        path / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()
    # fractal_ssh.send_file(local=local_wheel_path, remote=wheel_path)
    with open(local_wheel_path, "rb") as wheel_file:
        task_group = TaskGroupV2(
            pkg_name="fractal_tasks_mock",
            version="0.0.1",
            origin=TaskGroupV2OriginEnum.WHEELFILE,
            wheel_path=wheel_path,
            path=path.as_posix(),
            venv_path=venv_path.as_posix(),
            user_id=first_user.id,
            pip_freeze="pip",
            python_version=current_py_version,
        )
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        activity_collect = TaskGroupActivityV2(
            user_id=first_user.id,
            taskgroupv2_id=task_group.id,
            status=TaskGroupActivityStatusV2.PENDING,
            action=TaskGroupActivityActionV2.COLLECT,
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
            fractal_ssh=fractal_ssh,
            tasks_base_dir=tmp777_path.as_posix(),
            wheel_buffer=wheel_file.read(),
            wheel_filename=Path(wheel_path).name,
        )
        activity_collect = await db.get(
            TaskGroupActivityV2, activity_collect.id
        )
        assert activity_collect.status == TaskGroupActivityStatusV2.OK

        # STEP 2: make it look like a pre-2.9.0 package, both in the db and
        # in the virtual environment
        task_group = await db.get(TaskGroupV2, task_group.id)
        task_group.pip_freeze = None
        task_group.wheel_path = wheel_path
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        db.expunge(task_group)
        python_bin = (venv_path / "bin/python").as_posix()
        pip_install_new_wheel = (
            f"{python_bin} -m pip install {wheel_path} --force-reinstall"
        )
        fractal_ssh.run_command(cmd=pip_install_new_wheel)

        # STEP 3: Deactivate the task group
        activity_deactivate = TaskGroupActivityV2(
            user_id=first_user.id,
            taskgroupv2_id=task_group.id,
            status=TaskGroupActivityStatusV2.PENDING,
            action=TaskGroupActivityActionV2.DEACTIVATE,
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
            fractal_ssh=fractal_ssh,
            tasks_base_dir=tmp777_path.as_posix(),
        )

        # Check outcome
        activity_deactivate = await db.get(
            TaskGroupActivityV2, activity_deactivate.id
        )
        task_group = await db.get(TaskGroupV2, task_group.id)
        assert activity_deactivate.status == TaskGroupActivityStatusV2.OK
        print(activity_deactivate.log)
        assert "Recreate pip-freeze information" in activity_deactivate.log