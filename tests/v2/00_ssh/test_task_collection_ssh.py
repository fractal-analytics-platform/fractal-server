import io
from pathlib import Path

import pytest
from devtools import debug  # noqa: F401
from fabric.connection import Connection

from fractal_server.app.models.v2.collection_state import CollectionStateV2
from fractal_server.ssh._fabric import _mkdir_over_ssh
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2.background_operations_ssh import (
    background_collect_pip_ssh,
)


@pytest.fixture
def ssh_connection(
    slurmlogin_ip,
    ssh_alive,
    ssh_keys,
    monkeypatch,
):
    ssh_private_key = ssh_keys["private"]
    monkeypatch.setattr("sys.stdin", io.StringIO(""))
    with Connection(
        host=slurmlogin_ip,
        user="fractal",
        connect_kwargs={"key_filename": ssh_private_key},
    ) as connection:
        fractal_conn = FractalSSH(connection=connection)
        yield fractal_conn


async def test_task_collection_ssh(
    ssh_connection,
    db,
    override_settings_factory,
    tmp777_path: Path,
):

    remote_basedir = (tmp777_path / "WORKING_BASE_DIR").as_posix()
    debug(remote_basedir)

    _mkdir_over_ssh(
        folder=remote_basedir, connection=ssh_connection, parents=True
    )

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
        package_version="1.0.2",
        python_version="3.9",
    )

    background_collect_pip_ssh(
        state_id=state.id,
        task_pkg=task_pkg,
        connection=ssh_connection,
    )

    await db.refresh(state)
    debug(state)
