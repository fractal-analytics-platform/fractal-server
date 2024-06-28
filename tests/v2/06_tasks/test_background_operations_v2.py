from pathlib import Path
from typing import Optional

from devtools import debug

from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.routes.api.v2.task_collection import (
    TaskCollectStatusV2,
)
from fractal_server.config import get_settings
from fractal_server.syringe import Inject
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2.background_operations import _get_task_type
from fractal_server.tasks.v2.background_operations import (
    background_collect_pip,
)
from fractal_server.tasks.v2.endpoint_operations import (
    create_package_dir_pip,
)
from fractal_server.tasks.v2.endpoint_operations import inspect_package


def test_get_task_type():
    from pydantic import BaseModel

    class _MockTaskCreateV2(BaseModel):
        command_non_parallel: Optional[str] = None
        command_parallel: Optional[str] = None

    task = _MockTaskCreateV2(command_non_parallel="x")
    assert _get_task_type(task) == "non_parallel"
    task = _MockTaskCreateV2(command_parallel="x")
    assert _get_task_type(task) == "parallel"
    task = _MockTaskCreateV2(command_parallel="x", command_non_parallel="y")
    assert _get_task_type(task) == "compound"


async def test_logs_failed_collection(
    db,
    tmp_path: Path,
    testdata_path: Path,
):
    """
    GIVEN a package and its installation environment
    WHEN the background collection is called on it and it fails
    THEN
        * the log of the collection is saved to the state
        * the installation directory is removed
    """

    settings = Inject(get_settings)
    PYTHON_VERSION = settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
    task_package = (
        testdata_path
        / "../v2/fractal_tasks_mock/dist"
        / "fractal_tasks_mock-0.0.1-py3-none-any.whl"
    )

    # FAILURE 1: corrupted `package_path`

    # Preliminary steps
    task_pkg = _TaskCollectPip(
        package=task_package.as_posix(), python_version=PYTHON_VERSION
    )
    pkg_info = inspect_package(task_pkg.package_path)
    task_pkg.package_version = pkg_info["pkg_version"]
    task_pkg.package_manifest = pkg_info["pkg_manifest"]
    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    collection_status = TaskCollectStatusV2(
        status="pending", venv_path=venv_path, package=task_pkg.package
    )
    state = CollectionStateV2(data=collection_status.sanitised_dict())
    db.add(state)
    await db.commit()
    await db.refresh(state)
    # Introduce failure (corrupt package_path)
    task_pkg.package_path = tmp_path / "something-wrong.whl"
    # Run background collection and check that failure was recorded
    await background_collect_pip(
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )
    await db.refresh(state)
    debug(state.data["status"])
    assert state.data["log"]
    assert state.data["status"] == "fail"
    assert state.data["info"].startswith("Original error")
    assert not venv_path.exists()
    assert "is not a valid wheel filename" in state.data["log"]

    # FAILURE 2: corrupted `package_version` and failure of `check()`

    # Preliminary steps
    task_pkg = _TaskCollectPip(
        package=task_package.as_posix(), python_version=PYTHON_VERSION
    )
    task_pkg.package_version = pkg_info["pkg_version"]
    task_pkg.package_manifest = pkg_info["pkg_manifest"]
    venv_path = create_package_dir_pip(task_pkg=task_pkg)
    collection_status = TaskCollectStatusV2(
        status="pending", venv_path=venv_path, package=task_pkg.package
    )
    state = CollectionStateV2(data=collection_status.sanitised_dict())
    db.add(state)
    await db.commit()
    await db.refresh(state)
    # Introduce failure (corrupt package_path)
    task_pkg.package_version = None
    # Run background collection and check that failure was recorded
    await background_collect_pip(
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )
    await db.refresh(state)
    debug(state.data["status"])
    assert state.data["log"]
    assert state.data["status"] == "fail"
    assert state.data["info"].startswith("Original error")
    assert not venv_path.exists()
    assert "`package_version` attribute is not set" in state.data["log"]
