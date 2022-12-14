import asyncio
import json
from copy import deepcopy
from pathlib import Path
from shutil import copy as shell_copy
from shutil import rmtree as shell_rmtree
from tempfile import TemporaryDirectory
from typing import List
from typing import Optional

from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from sqlmodel import select

from ....common.schemas import TaskCollectPip
from ....common.schemas import TaskCollectStatus
from ....common.schemas import TaskCreate
from ....common.schemas import TaskRead
from ....common.schemas import TaskUpdate
from ....config import get_settings
from ....syringe import Inject
from ....tasks.collection import _TaskCollectPip
from ....tasks.collection import create_package_dir_pip
from ....tasks.collection import create_package_environment_pip
from ....tasks.collection import download_package
from ....tasks.collection import get_collection_data
from ....tasks.collection import get_collection_log
from ....tasks.collection import get_collection_path
from ....tasks.collection import inspect_package
from ....tasks.collection import TaskCollectionError
from ....utils import set_logger
from ...db import AsyncSession
from ...db import DBSyncSession
from ...db import get_db
from ...db import get_sync_db
from ...models import State
from ...models import Task
from ...security import current_active_user
from ...security import User

router = APIRouter()


async def _background_collect_pip(
    state: State, venv_path: Path, task_pkg: _TaskCollectPip, db: AsyncSession
) -> List[Task]:
    """
    Install package and collect tasks

    Install a python package and collect the tasks it provides according to
    the manifest.

    In case of error, copy the log into the state and delete the package
    directory.
    """
    logger = set_logger(logger_name="fractal")
    data = TaskCollectStatus(**state.data)
    data.info = None

    try:
        # install
        logger.info("installing")
        data.status = "installing"

        state.data = data.sanitised_dict()
        await db.merge(state)
        await db.commit()
        task_list = await create_package_environment_pip(
            venv_path=venv_path, task_pkg=task_pkg
        )

        # collect
        logger.info("collecting")
        data.status = "collecting"
        state.data = data.sanitised_dict()
        await db.merge(state)
        await db.commit()
        tasks = await _insert_tasks(task_list=task_list, db=db)

        # finalise
        logger.info("finalising")
        collection_path = get_collection_path(venv_path)
        data.task_list = tasks
        with collection_path.open("w") as f:
            json.dump(data.sanitised_dict(), f)

        data.status = "OK"
        data.task_list = tasks
        state.data = data.sanitised_dict()
        db.add(state)
        await db.merge(state)
        await db.commit()

        logger.info("background collection completed")
        return tasks
    except Exception as e:
        err = TaskCollectionError(*e.args)
        data.status = "fail"
        data.info = f"Original error: {e}"
        data.log = get_collection_log(venv_path)
        state.data = data.sanitised_dict()
        await db.merge(state)
        await db.commit()

        # delete corrupted package dir
        shell_rmtree(venv_path)
        raise err


async def _insert_tasks(
    task_list: List[TaskCreate],
    db: AsyncSession,
) -> List[Task]:
    """
    Insert tasks into database
    """
    task_db_list = [Task.from_orm(t) for t in task_list]
    db.add_all(task_db_list)
    await db.commit()
    await asyncio.gather(*[db.refresh(t) for t in task_db_list])
    return task_db_list


@router.post(
    "/collect/pip/",
    response_model=State,
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
    public: bool = True,
) -> State:  # State[TaskCollectStatus]
    """
    Task collection endpoint

    Trigger the creation of a dedicated virtual environment, the installation
    of a package and the collection of tasks as advertised in the manifest.
    """

    logger = set_logger(logger_name="fractal")
    task_pkg = _TaskCollectPip(**task_collect.dict())

    with TemporaryDirectory() as tmpdir:
        try:
            if task_pkg.is_local_package:
                shell_copy(task_pkg.package_path.as_posix(), tmpdir)
                pkg_path = Path(tmpdir) / task_pkg.package_path.name
            else:
                pkg_path = await download_package(
                    task_pkg=task_pkg, dest=tmpdir
                )

            version_manifest = inspect_package(pkg_path)

            task_pkg.version = version_manifest["version"]
            task_pkg.check()
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Invalid package or manifest. Original error: {e}",
            )

    try:
        pkg_user = None if public else user.slurm_user
        venv_path = create_package_dir_pip(task_pkg=task_pkg, user=pkg_user)
    except FileExistsError:
        venv_path = create_package_dir_pip(
            task_pkg=task_pkg, user=pkg_user, create=False
        )
        task_collect_status = get_collection_data(venv_path)
        task_collect_status.info = "Already installed"
        state = State(data=task_collect_status.sanitised_dict())
        response.status_code == status.HTTP_200_OK
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

    logger.info("starting background collection")
    background_tasks.add_task(
        _background_collect_pip,
        state=state,
        venv_path=venv_path,
        task_pkg=task_pkg,
        db=db,
    )

    logger.info("collection endpoint: returning state")
    info = (
        "Collecting tasks in the background. "
        "GET /task/collect/{id} to query collection status"
    )
    state.data["info"] = info
    response.status_code = status.HTTP_201_CREATED
    return state


@router.get("/collect/{state_id}", response_model=State)
async def check_collection_status(
    state_id: int,
    user: User = Depends(current_active_user),
    verbose: bool = False,
    db: AsyncSession = Depends(get_db),
) -> State:  # State[TaskCollectStatus]
    logger = set_logger(logger_name="fractal")
    logger.info("querying state")
    state = await db.get(State, state_id)
    if not state:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No task collection info with id={state_id}",
        )
    data = TaskCollectStatus(**state.data)

    # collection_path = get_collection_path(package_path)
    if verbose:
        data.log = get_collection_log(data.venv_path)
        state.data = data.sanitised_dict()
    return state


@router.get("/", response_model=List[TaskRead])
async def get_list_task(
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> List[TaskRead]:
    stm = select(Task)
    res = await db.execute(stm)
    task_list = res.scalars().unique().fetchall()
    await asyncio.gather(*[db.refresh(t) for t in task_list])
    return task_list


@router.get("/{task_id}", response_model=TaskRead)
def get_task(
    task_id: int,
    user: User = Depends(current_active_user),
    db_sync: DBSyncSession = Depends(get_sync_db),
) -> TaskRead:
    task = db_sync.get(Task, task_id)
    return task


@router.patch("/{task_id}", response_model=TaskRead)
async def patch_task(
    task_id: int,
    task_update: TaskUpdate,
    user: User = Depends(current_active_user),
    db: AsyncSession = Depends(get_db),
) -> Optional[TaskRead]:

    # FIXME add user-owned tasks

    db_task = await db.get(Task, task_id)

    for key, value in task_update.dict(exclude_unset=True).items():
        if key == "name":
            setattr(db_task, key, value)
        elif key == "default_args":
            current_default_args = deepcopy(db_task._arguments)
            current_default_args.update(value)
            setattr(db_task, key, current_default_args)
        else:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="patch_task endpoint cannot set {key=}",
            )

    await db.commit()
    await db.refresh(db_task)
    return db_task
