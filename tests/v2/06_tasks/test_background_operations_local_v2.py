import shlex
import subprocess
from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.tasks.v2.backgroud_operations_local import (
    background_collect_pip_local,
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


async def test_background_collect_pip_existing_file(
    tmp_path, db, first_user, testdata_path, tmpdir_factory, current_py_version
):

    base_dir = Path(tmpdir_factory.getbasetemp())
    venv_dir = base_dir / "task_venv"

    if not venv_dir.exists():
        run_cmd(f"python{current_py_version} -m venv {venv_dir}")
    debug(base_dir)
    debug(venv_dir)
    # Prepare db objects
    path = tmp_path / "something"
    task_group = TaskGroupV2(
        pkg_name="fractal-tasks-mock",
        version="0.0.1",
        python_version="3.10",
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
    # Run background task
    await background_collect_pip_local(
        task_group=task_group,
        state_id=state.id,
        # tasks_base_dir=tmp_path
    )
    state = await db.get(CollectionStateV2, state.id)
    debug(state)
    assert state.data["status"] == "OK"
