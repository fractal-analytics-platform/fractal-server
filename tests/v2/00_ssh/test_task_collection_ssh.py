from pathlib import Path

from devtools import debug

from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2.background_operations_ssh import (
    background_collect_pip_ssh,
)


async def test_task_collection_ssh(
    fractal_ssh: FractalSSH,
    db,
    override_settings_factory,
    tmp777_path: Path,
    current_py_version: str,
    first_user: UserOAuth,
):
    remote_basedir = (tmp777_path / "WORKING_BASE_DIR").as_posix()
    debug(remote_basedir)

    fractal_ssh.mkdir(
        folder=remote_basedir,
        parents=True,
    )

    CURRENT_FRACTAL_MAX_PIP_VERSION = "21.0"

    current_py_version_underscore = current_py_version.replace(".", "_")
    PY_KEY = f"FRACTAL_TASKS_PYTHON_{current_py_version_underscore}"
    setting_overrides = {
        "FRACTAL_SLURM_WORKER_PYTHON": f"/usr/bin/python{current_py_version}",
        PY_KEY: f"/usr/bin/python{current_py_version}",
        "FRACTAL_MAX_PIP_VERSION": CURRENT_FRACTAL_MAX_PIP_VERSION,
    }
    override_settings_factory(**setting_overrides)

    # CASE 1: successful collection
    state = CollectionStateV2()
    db.add(state)
    await db.commit()
    await db.refresh(state)
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-core",
        version="1.0.2",
        origin="pypi",
        path=(Path(remote_basedir) / "fractal-tasks-core/1.0.2").as_posix(),
        venv_path=(
            Path(remote_basedir) / "fractal-tasks-core/1.0.2/venv"
        ).as_posix(),
        python_version=current_py_version,
        user_id=first_user.id,
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)

    background_collect_pip_ssh(
        state_id=state.id,
        task_group=task_group,
        fractal_ssh=fractal_ssh,
        tasks_base_dir=remote_basedir,
    )
    await db.refresh(state)
    debug(state)
    assert state.data["status"] == "OK"

    # Check that pip version contraint is valid
    pip_version = next(
        line
        for line in state.data["freeze"].split("\n")
        if line.startswith("pip")
    ).split("==")[1]
    assert pip_version == CURRENT_FRACTAL_MAX_PIP_VERSION

    # Check that the remote folder exists (note: we can do it on the host
    # machine, because /tmp is shared with the container)
    venv_dir = Path(task_group.venv_path)
    debug(venv_dir)
    assert venv_dir.is_dir()

    # CASE 2: Try collecting the same package again
    state = CollectionStateV2()
    db.add(state)
    await db.commit()
    await db.refresh(state)
    background_collect_pip_ssh(
        state_id=state.id,
        task_group=task_group,
        fractal_ssh=fractal_ssh,
        tasks_base_dir=remote_basedir,
    )

    # Check that the second collection failed, since folder already exists
    await db.refresh(state)
    debug(state)
    assert state.data["status"] == "fail"
    assert "already exists" in state.data["log"]
    # Check that the remote folder was not removed (note: we can do it on the
    # host machine, because /tmp is shared with the container)
    venv_dir = Path(task_group.venv_path)
    debug(venv_dir)
    assert venv_dir.is_dir()

    # CASE 3: Fail due to wrong version
    task_group_invalid = TaskGroupV2(
        pkg_name="fractal-tasks-core",
        version="999.999.999",
        origin="pypi",
        path=(
            Path(remote_basedir) / "fractal-tasks-core/999.999.999"
        ).as_posix(),
        venv_path=(
            Path(remote_basedir) / "fractal-tasks-core/999.999.999/venv"
        ).as_posix(),
        python_version=current_py_version,
        user_id=first_user.id,
    )
    db.add(task_group_invalid)
    await db.commit()
    await db.refresh(task_group_invalid)
    db.expunge(task_group_invalid)

    state = CollectionStateV2()
    db.add(state)
    await db.commit()
    await db.refresh(state)
    background_collect_pip_ssh(
        state_id=state.id,
        task_group=task_group_invalid,
        fractal_ssh=fractal_ssh,
        tasks_base_dir=remote_basedir,
    )
    await db.refresh(state)
    debug(state)
    assert state.data["status"] == "fail"
    assert "Could not find a version" in state.data["log"]

    # Check that the remote folder does not exist (note: we can do it on the
    # host machine, because /tmp is shared with the container)
    venv_dir = Path(task_group_invalid.venv_path)
    debug(venv_dir)
    assert not venv_dir.is_dir()
