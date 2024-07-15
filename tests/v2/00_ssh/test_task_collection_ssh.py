from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2.collection_state import CollectionStateV2
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2.background_operations_ssh import (
    background_collect_pip_ssh,
)


async def test_task_collection_ssh(
    fractal_ssh: FractalSSH,
    db,
    override_settings_factory,
    tmp777_path: Path,
):

    remote_basedir = (tmp777_path / "WORKING_BASE_DIR").as_posix()
    debug(remote_basedir)

    fractal_ssh.mkdir(
        folder=remote_basedir,
        parents=True,
    )

    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON="/usr/bin/python3.9",
        FRACTAL_TASKS_PYTHON_3_9="/usr/bin/python3.9",
        FRACTAL_SLURM_SSH_WORKING_BASE_DIR=remote_basedir,
    )

    # CASE 1: successful collection
    state = CollectionStateV2()
    db.add(state)
    await db.commit()
    await db.refresh(state)
    task_pkg = _TaskCollectPip(
        package="fractal_tasks_core",
        package_version="1.0.2",
        python_version="3.9",
    )
    background_collect_pip_ssh(
        state_id=state.id,
        task_pkg=task_pkg,
        fractal_ssh=fractal_ssh,
    )
    await db.refresh(state)
    debug(state)
    assert state.data["status"] == "OK"

    # Check that the remote folder exists (note: we can do it on the host
    # machine, because /tmp is shared with the container)
    venv_dir = Path(remote_basedir) / ".fractal/fractal-tasks-core1.0.2"
    debug(venv_dir)
    assert venv_dir.is_dir()

    # CASE 2: Try collecting the same package again
    state = CollectionStateV2()
    db.add(state)
    await db.commit()
    await db.refresh(state)
    background_collect_pip_ssh(
        state_id=state.id,
        task_pkg=task_pkg,
        fractal_ssh=fractal_ssh,
    )

    # Check that the second collection failed, since folder already exists
    await db.refresh(state)
    debug(state)
    assert state.data["status"] == "fail"
    assert "already exists" in state.data["log"]
    # Check that the remote folder was not removed (note: we can do it on the
    # host machine, because /tmp is shared with the container)
    venv_dir = Path(remote_basedir) / ".fractal/fractal-tasks-core1.0.2"
    debug(venv_dir)
    assert venv_dir.is_dir()


async def test_task_collection_ssh_failure(
    fractal_ssh: FractalSSH,
    db,
    override_settings_factory,
    tmp777_path: Path,
):

    remote_basedir = (tmp777_path / "WORKING_BASE_DIR").as_posix()
    debug(remote_basedir)

    fractal_ssh.mkdir(folder=remote_basedir, parents=True)

    override_settings_factory(
        FRACTAL_SLURM_WORKER_PYTHON="/usr/bin/python3.9",
        FRACTAL_TASKS_PYTHON_3_9="/usr/bin/python3.9",
        FRACTAL_SLURM_SSH_WORKING_BASE_DIR=remote_basedir,
    )

    state = CollectionStateV2()
    db.add(state)
    await db.commit()
    await db.refresh(state)

    task_pkg = _TaskCollectPip(
        package="fractal_tasks_core",
        package_version="99.99.99",
        python_version="3.9",
    )

    background_collect_pip_ssh(
        state_id=state.id,
        task_pkg=task_pkg,
        fractal_ssh=fractal_ssh,
    )

    await db.refresh(state)
    debug(state)
    assert state.data["status"] == "fail"
    assert "Could not find a version" in state.data["log"]

    # Check that the remote folder does not exist (note: we can do it on the
    # host machine, because /tmp is shared with the container)
    venv_dir = Path(remote_basedir) / ".fractal/fractal-tasks-core99.99.99"
    debug(venv_dir)
    assert not venv_dir.is_dir()
