from pathlib import Path
from shutil import copy as shell_copy
from tempfile import TemporaryDirectory

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic.error_wrappers import ValidationError
from sqlmodel import select

from .....config import get_settings
from .....logger import close_logger
from .....logger import set_logger
from .....syringe import Inject
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v1 import State
from ....models.v1 import Task
from ....schemas.v1 import StateRead
from ....schemas.v1 import TaskCollectPipV1
from ....schemas.v1 import TaskCollectStatusV1
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User
from fractal_server.tasks.utils import get_collection_log
from fractal_server.tasks.utils import slugify_task_name
from fractal_server.tasks.v1._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v1.background_operations import (
    background_collect_pip,
)
from fractal_server.tasks.v1.endpoint_operations import create_package_dir_pip
from fractal_server.tasks.v1.endpoint_operations import download_package
from fractal_server.tasks.v1.endpoint_operations import inspect_package
from fractal_server.tasks.v1.get_collection_data import get_collection_data

router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/pip/",
    response_model=StateRead,
    responses={
        201: dict(
            description=(
                "Task collection successfully started in the background"
            )
        ),
        200: dict(
            description=(
                "Package already collected. Returning info on already "
                "available tasks"
            )
        ),
    },
)
async def collect_tasks_pip(
    task_collect: TaskCollectPipV1,
    background_tasks: BackgroundTasks,
    response: Response,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> StateRead:  # State[TaskCollectStatus]
    """
    Task collection endpoint

    Trigger the creation of a dedicated virtual environment, the installation
    of a package and the collection of tasks as advertised in the manifest.
    """

    logger = set_logger(logger_name="collect_tasks_pip")

    # Validate payload as _TaskCollectPip, which has more strict checks than
    # TaskCollectPip
    try:
        task_pkg = _TaskCollectPip(**task_collect.dict(exclude_unset=True))
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Invalid task-collection object. Original error: {e}",
        )

    with TemporaryDirectory() as tmpdir:
        try:
            # Copy or download the package wheel file to tmpdir
            if task_pkg.is_local_package:
                shell_copy(task_pkg.package_path.as_posix(), tmpdir)
                pkg_path = Path(tmpdir) / task_pkg.package_path.name
            else:
                pkg_path = await download_package(
                    task_pkg=task_pkg, dest=tmpdir
                )
            # Read package info from wheel file, and override the ones coming
            # from the request body
            pkg_info = inspect_package(pkg_path)
            task_pkg.package_name = pkg_info["pkg_name"]
            task_pkg.package_version = pkg_info["pkg_version"]
            task_pkg.package_manifest = pkg_info["pkg_manifest"]
            task_pkg.check()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid package or manifest. Original error: {e}",
            )

    try:
        venv_path = create_package_dir_pip(task_pkg=task_pkg)
    except FileExistsError:
        venv_path = create_package_dir_pip(task_pkg=task_pkg, create=False)
        try:
            task_collect_status = get_collection_data(venv_path)
            for task in task_collect_status.task_list:
                db_task = await db.get(Task, task.id)
                if (
                    (not db_task)
                    or db_task.source != task.source
                    or db_task.name != task.name
                ):
                    await db.close()
                    raise HTTPException(
                        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                        detail=(
                            "Cannot collect package. Folder already exists, "
                            f"but task {task.id} does not exists or it does "
                            f"not have the expected source ({task.source}) or "
                            f"name ({task.name})."
                        ),
                    )
        except FileNotFoundError as e:
            await db.close()
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot collect package. Possible reason: another "
                    "collection of the same package is in progress. "
                    f"Original error: {e}"
                ),
            )
        except ValidationError as e:
            await db.close()
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot collect package. Possible reason: an old version "
                    "of the same package has already been collected. "
                    f"Original error: {e}"
                ),
            )
        task_collect_status.info = "Already installed"
        state = State(data=task_collect_status.sanitised_dict())
        response.status_code == status.HTTP_200_OK
        await db.close()
        return state
    settings = Inject(get_settings)

    # Check that tasks are not already in the DB
    for new_task in task_pkg.package_manifest.task_list:
        new_task_name_slug = slugify_task_name(new_task.name)
        new_task_source = f"{task_pkg.package_source}:{new_task_name_slug}"
        stm = select(Task).where(Task.source == new_task_source)
        res = await db.execute(stm)
        if res.scalars().all():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot collect package. Task with source "
                    f'"{new_task_source}" already exists in the database.'
                ),
            )

    # All checks are OK, proceed with task collection
    full_venv_path = venv_path.relative_to(settings.FRACTAL_TASKS_DIR)
    collection_status = TaskCollectStatusV1(
        status="pending", venv_path=full_venv_path, package=task_pkg.package
    )

    # Create State object (after casting venv_path to string)
    collection_status_dict = collection_status.dict()
    collection_status_dict["venv_path"] = str(collection_status.venv_path)
    state = State(data=collection_status_dict)
    db.add(state)
    await db.commit()
    await db.refresh(state)

    background_tasks.add_task(
        background_collect_pip,
        state_id=state.id,
        venv_path=venv_path,
        task_pkg=task_pkg,
    )
    logger.debug(
        "Task-collection endpoint: start background collection "
        "and return state"
    )
    close_logger(logger)
    info = (
        "Collecting tasks in the background. "
        f"GET /task/collect/{state.id} to query collection status"
    )
    state.data["info"] = info
    response.status_code = status.HTTP_201_CREATED
    await db.close()
    return state


@router.get("/collect/{state_id}/", response_model=StateRead)
async def check_collection_status(
    state_id: int,
    user: User = Depends(current_active_user),
    verbose: bool = False,
    db: AsyncSession = Depends(get_async_db),
) -> StateRead:  # State[TaskCollectStatus]
    """
    Check status of background task collection
    """
    logger = set_logger(logger_name="check_collection_status")
    logger.debug(f"Querying state for state.id={state_id}")
    state = await db.get(State, state_id)
    if not state:
        await db.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No task collection info with id={state_id}",
        )
    data = TaskCollectStatusV1(**state.data)

    # In some cases (i.e. a successful or ongoing task collection), data.log is
    # not set; if so, we collect the current logs
    if verbose and not data.log:
        data.log = get_collection_log(data.venv_path)
        state.data = data.sanitised_dict()
    close_logger(logger)
    await db.close()
    return state
