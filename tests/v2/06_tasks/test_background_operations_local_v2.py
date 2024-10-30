import shlex
import subprocess
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.tasks.v2.collection_local import (
    collect_package_local,
)


def run_cmd(cmd: str):
    res = subprocess.run(  # nosec
        shlex.split(cmd),
        capture_output=True,
        encoding="utf8",
    )
    if res.returncode != 0:
        raise subprocess.CalledProcessError(
            res.returncode, cmd=cmd, output=res.stdout, stderr=res.stderr
        )
    return res.stdout


async def test_background_collect_pip(
    tmp_path, db, first_user, testdata_path, tmpdir_factory, current_py_version
):

    path = tmp_path / "path"
    venv_path = path / "venv"

    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-mock",
        version="0.0.1",
        user_id=first_user.id,
        python_version=current_py_version,
        wheel_path=(
            f"{testdata_path.parent}/v2/fractal_tasks_valid/valid_tasks/dist"
            "/fractal_tasks_mock-0.0.1-py3-none-any.whl"
        ),
        path=path.as_posix(),
        venv_path=venv_path.as_posix(),
        origin="wheel-path",
    )
    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    state = CollectionStateV2(taskgroupv2_id=task_group.id)
    db.add(state)
    await db.commit()
    await db.refresh(state)
    db.expunge(state)
    # Run background task
    await collect_package_local(
        task_group=task_group,
        state_id=state.id,
    )
    state = await db.get(CollectionStateV2, state.id)
    debug(state)
    assert state.data["status"] == "OK"


async def test_background_collect_pip_existing_file(
    tmp_path, db, first_user, current_py_version, tmpdir_factory, testdata_path
):

    base_dir = Path(tmpdir_factory.getbasetemp())
    venv_dir = base_dir / "task_venv"

    if not venv_dir.exists():
        import subprocess
        import shlex

        subprocess.run(
            shlex.split(f"python{current_py_version} -m venv {venv_dir}")
        )

    debug(base_dir)
    debug(venv_dir)

    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-mock",
        version="0.0.1",
        python_version=current_py_version,
        wheel_path=(
            f"{testdata_path.parent}/v2/fractal_tasks_valid/valid_tasks/dist"
            "/fractal_tasks_mock-0.0.1-py3-none-any.whl"
        ),
        venv_path=venv_dir.as_posix(),
        origin="local",
        path=path.as_posix(),
        user_id=first_user.id,
    )

    db.add(task_group)
    await db.commit()
    await db.refresh(task_group)
    db.expunge(task_group)
    state = CollectionStateV2(taskgroupv2_id=task_group.id)
    db.add(state)
    await db.commit()
    await db.refresh(state)
    db.expunge(state)
    # Create task_group.path
    path.mkdir()
    debug(path)
    # Run background task
    with pytest.raises(FileExistsError):
        await collect_package_local(
            task_group=task_group,
            state_id=state.id,
        )
    # Verify that collection failed
    # state = await db.get(CollectionStateV2, state.id)
    # debug(state)
    #
    # assert state.data["status"] == "fail"
    # assert "already exists" in state.data["log"]
    # # Verify that foreign key was set to None
    # assert state.taskgroupv2_id is None
