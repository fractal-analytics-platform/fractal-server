import json
from pathlib import Path
from shutil import copy as shell_copy
from shutil import rmtree as shell_rmtree
from tempfile import TemporaryDirectory

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic.error_wrappers import ValidationError

from ....common.schemas import StateRead
from ....common.schemas import TaskCollectPip
from ....common.schemas import TaskCollectStatus
from ....common.schemas import TaskCreate
from ....config import get_settings
from ....logger import close_logger
from ....logger import set_logger
from ....syringe import Inject
from ....tasks.collection import _TaskCollectPip
from ....tasks.collection import create_package_dir_pip
from ....tasks.collection import create_package_environment_pip
from ....tasks.collection import download_package
from ....tasks.collection import get_collection_data
from ....tasks.collection import get_collection_log
from ....tasks.collection import get_collection_path
from ....tasks.collection import get_log_path
from ....tasks.collection import inspect_package
from ...db import AsyncSession
from ...db import DBSyncSession
from ...db import get_db
from ...db import get_sync_db
from ...models import State
from ...models import Task
from ...security import current_active_user
from ...security import User

router = APIRouter()

logger = set_logger(__name__)


async def _background_collect_pip(
    state_id: int,
    venv_path: Path,
    task_pkg: _TaskCollectPip,
) -> None:
    """
    Install package and collect tasks

    Install a python package and collect the tasks it provides according to
    the manifest.

    In case of error, copy the log into the state and delete the package
    directory.
    """
    logger_name = task_pkg.package.replace("/", "_")
    logger = set_logger(
        logger_name=logger_name,
        log_file_path=get_log_path(venv_path),
    )
    logger.debug("Start background task collection")
    for key, value in task_pkg.dict(exclude={"package_manifest"}).items():
        logger.debug(f"{key}: {value}")

    with next(get_sync_db()) as db:
        state: State = db.get(State, state_id)
        data = TaskCollectStatus(**state.data)
        data.info = None

        try:
            # install
            logger.debug("Task-collection status: installing")
            data.status = "installing"

            state.data = data.sanitised_dict()
            db.merge(state)
            db.commit()
            task_list = await create_package_environment_pip(
                venv_path=venv_path,
                task_pkg=task_pkg,
                logger_name=logger_name,
            )

            # collect
            logger.debug("Task-collection status: collecting")
            data.status = "collecting"
            state.data = data.sanitised_dict()
            db.merge(state)
            db.commit()
            tasks = await _insert_tasks(task_list=task_list, db=db)

            # finalise
            logger.debug("Task-collection status: finalising")
            collection_path = get_collection_path(venv_path)
            data.task_list = tasks
            with collection_path.open("w") as f:
                json.dump(data.sanitised_dict(), f)

            # Update DB
            data.status = "OK"
            data.log = get_collection_log(venv_path)
            state.data = data.sanitised_dict()
            db.add(state)
            db.merge(state)
            db.commit()

            # Write last logs to file
            logger.debug("Task-collection status: OK")
            logger.info("Background task collection completed successfully")
            close_logger(logger)
            db.close()

        except Exception as e:
            # Write last logs to file
            logger.debug("Task-collection status: fail")
            logger.info(f"Background collection failed. Original error: {e}")
            close_logger(logger)

            # Update db
            data.status = "fail"
            data.info = f"Original error: {e}"
            data.log = get_collection_log(venv_path)
            state.data = data.sanitised_dict()
            db.merge(state)
            db.commit()
            db.close()

            # Delete corrupted package dir
            shell_rmtree(venv_path)


async def _insert_tasks(
    task_list: list[TaskCreate],
    db: DBSyncSession,
) -> list[Task]:
    """
    Insert tasks into database
    """
    task_db_list = [Task.from_orm(t) for t in task_list]
    db.add_all(task_db_list)
    db.commit()
    for t in task_db_list:
        db.refresh(t)
    db.close()
    return task_db_list


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
    task_collect: TaskCollectPip,
    background_tasks: BackgroundTasks,
    response: Response,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
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
        task_collect_status.info = "Already installed"
        state = State(data=task_collect_status.sanitised_dict())
        response.status_code == status.HTTP_200_OK
        await db.close()
        return state
    settings = Inject(get_settings)

    full_venv_path = venv_path.relative_to(settings.FRACTAL_TASKS_DIR)
    collection_status = TaskCollectStatus(
        status="pending", venv_path=full_venv_path, package=task_pkg.package
    )
    # replacing with path because of non-serializable Path
    collection_status_dict = collection_status.dict()
    collection_status_dict["venv_path"] = str(collection_status.venv_path)

    state = State(data=collection_status_dict)
    db.add(state)
    await db.commit()
    await db.refresh(state)

    background_tasks.add_task(
        _background_collect_pip,
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


@router.get("/collect/{state_id}", response_model=StateRead)
async def check_collection_status(
    state_id: int,
    user: User = Depends(current_active_user),
    verbose: bool = False,
    db: AsyncSession = Depends(get_db),
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
    data = TaskCollectStatus(**state.data)

    # In some cases (i.e. a successful or ongoing task collection), data.log is
    # not set; if so, we collect the current logs
    if verbose and not data.log:
        data.log = get_collection_log(data.venv_path)
        state.data = data.sanitised_dict()
    close_logger(logger)
    await db.close()
    return state
