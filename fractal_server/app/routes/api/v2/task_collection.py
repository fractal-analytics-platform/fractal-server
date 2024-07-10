import json
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
from .....logger import reset_logger_handlers
from .....logger import set_logger
from .....syringe import Inject
from ....db import AsyncSession
from ....db import get_async_db
from ....models.v2 import CollectionStateV2
from ....models.v2 import TaskV2
from ....schemas.v2 import CollectionStateReadV2
from ....schemas.v2 import CollectionStatusV2
from ....schemas.v2 import TaskCollectPipV2
from ....schemas.v2 import TaskReadV2
from ....security import current_active_user
from ....security import current_active_verified_user
from ....security import User
from fractal_server.tasks.utils import get_absolute_venv_path
from fractal_server.tasks.utils import get_collection_log
from fractal_server.tasks.utils import get_collection_path
from fractal_server.tasks.utils import slugify_task_name
from fractal_server.tasks.v2._TaskCollectPip import _TaskCollectPip
from fractal_server.tasks.v2.background_operations import (
    background_collect_pip,
)
from fractal_server.tasks.v2.endpoint_operations import create_package_dir_pip
from fractal_server.tasks.v2.endpoint_operations import download_package
from fractal_server.tasks.v2.endpoint_operations import inspect_package


router = APIRouter()

logger = set_logger(__name__)


@router.post(
    "/collect/pip/",
    response_model=CollectionStateReadV2,
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
    task_collect: TaskCollectPipV2,
    background_tasks: BackgroundTasks,
    response: Response,
    user: User = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> CollectionStateReadV2:
    """
    Task collection endpoint

    Trigger the creation of a dedicated virtual environment, the installation
    of a package and the collection of tasks as advertised in the manifest.
    """

    logger = set_logger(logger_name="collect_tasks_pip")

    # Set default python version
    if task_collect.python_version is None:
        settings = Inject(get_settings)
        task_collect.python_version = (
            settings.FRACTAL_TASKS_PYTHON_DEFAULT_VERSION
        )

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
                wheel_path = Path(tmpdir) / task_pkg.package_path.name
            else:
                logger.info(f"Now download {task_pkg}")
                wheel_path = await download_package(
                    task_pkg=task_pkg, dest=tmpdir
                )
            # Read package info from wheel file, and override the ones coming
            # from the request body. Note that `package_name` was already set
            # (and normalized) as part of `_TaskCollectPip` initialization.
            pkg_info = inspect_package(wheel_path)
            task_pkg.package_version = pkg_info["pkg_version"]
            task_pkg.package_manifest = pkg_info["pkg_manifest"]
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
            package_path = get_absolute_venv_path(venv_path)
            collection_path = get_collection_path(package_path)
            with collection_path.open("r") as f:
                task_collect_data = json.load(f)

            err_msg = (
                "Cannot collect package, possible reason: an old version of "
                "the same package has already been collected.\n"
                f"{str(collection_path)} has invalid content: "
            )
            if not isinstance(task_collect_data, dict):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"{err_msg} it's not a Python dictionary.",
                )
            if "task_list" not in task_collect_data.keys():
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"{err_msg} it has no key 'task_list'.",
                )
            if not isinstance(task_collect_data["task_list"], list):
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"{err_msg} 'task_list' is not a Python list.",
                )

            for task_dict in task_collect_data["task_list"]:

                task = TaskReadV2(**task_dict)
                db_task = await db.get(TaskV2, task.id)
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
                    f"Original FileNotFoundError: {e}"
                ),
            )
        except ValidationError as e:
            await db.close()
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Cannot collect package. Possible reason: an old version "
                    "of the same package has already been collected. "
                    f"Original ValidationError: {e}"
                ),
            )
        task_collect_data["info"] = "Already installed"
        state = CollectionStateV2(data=task_collect_data)
        response.status_code == status.HTTP_200_OK
        await db.close()
        return state
    settings = Inject(get_settings)

    # Check that tasks are not already in the DB
    for new_task in task_pkg.package_manifest.task_list:
        new_task_name_slug = slugify_task_name(new_task.name)
        new_task_source = f"{task_pkg.package_source}:{new_task_name_slug}"
        stm = select(TaskV2).where(TaskV2.source == new_task_source)
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
    collection_status = dict(
        status=CollectionStatusV2.PENDING,
        venv_path=venv_path.relative_to(settings.FRACTAL_TASKS_DIR).as_posix(),
        package=task_pkg.package,
    )
    state = CollectionStateV2(data=collection_status)
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
    reset_logger_handlers(logger)
    info = (
        "Collecting tasks in the background. "
        f"GET /task/collect/{state.id} to query collection status"
    )
    state.data["info"] = info
    response.status_code = status.HTTP_201_CREATED
    await db.close()

    return state


@router.get("/collect/{state_id}/", response_model=CollectionStateReadV2)
async def check_collection_status(
    state_id: int,
    user: User = Depends(current_active_user),
    verbose: bool = False,
    db: AsyncSession = Depends(get_async_db),
) -> CollectionStateReadV2:  # State[TaskCollectStatus]
    """
    Check status of background task collection
    """
    logger = set_logger(logger_name="check_collection_status")
    logger.debug(f"Querying state for state.id={state_id}")
    state = await db.get(CollectionStateV2, state_id)
    if not state:
        await db.close()
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No task collection info with id={state_id}",
        )

    # In some cases (i.e. a successful or ongoing task collection),
    # state.data.log is not set; if so, we collect the current logs.
    if verbose and not state.data.get("log"):
        if "venv_path" not in state.data.keys():
            await db.close()
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"No 'venv_path' in CollectionStateV2[{state_id}].data",
            )
        state.data["log"] = get_collection_log(Path(state.data["venv_path"]))
        state.data["venv_path"] = str(state.data["venv_path"])
    reset_logger_handlers(logger)
    await db.close()
    return state
