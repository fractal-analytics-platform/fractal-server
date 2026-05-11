from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi import status
from pydantic import BaseModel
from pydantic import Field
from pydantic import Json

from fractal_server import __VERSION__
from fractal_server.app.db import AsyncSession
from fractal_server.app.db import get_async_db
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.routes.api.v2._aux_functions_task_lifecycle import (
    check_no_ongoing_activity,
)
from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _get_task_group_or_404,
)
from fractal_server.app.routes.auth import current_superuser_act
from fractal_server.app.routes.aux._python_interpreter import (
    get_python_interpreter_or_422,
)
from fractal_server.app.routes.aux.validate_user_profile import (
    validate_user_profile,
)
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2 import TaskGroupActivityAction
from fractal_server.app.schemas.v2 import TaskGroupActivityRead
from fractal_server.app.schemas.v2 import TaskGroupActivityStatus
from fractal_server.app.schemas.v2 import TaskGroupOriginEnum
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.local.reset import reset_local
from fractal_server.types import NonEmptyStr

router = APIRouter()

logger = set_logger(__name__)


class TaskGroupOverridesPip(BaseModel):
    """
    Overrides of the original task-group properties.

    Attributes:
        pip_extras:
        python_version:
        pinned_package_versions_pre:
        pinned_package_versions_post:
    """

    pip_extras: str | None = None
    python_version: NonEmptyStr | None = None
    pinned_package_versions_pre: Json[dict[NonEmptyStr, NonEmptyStr]] = Field(
        default_factory=dict
    )
    pinned_package_versions_post: Json[dict[NonEmptyStr, NonEmptyStr]] = Field(
        default_factory=dict
    )


@router.post(
    "/{task_group_id}/reset/pip/",
    response_model=TaskGroupActivityRead,
)
async def recollect_tasks_pip(
    task_group_id: int,
    response: Response,
    req_body: TaskGroupOverridesPip,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_async_db),
    _superuser: UserOAuth = Depends(current_superuser_act),
) -> TaskGroupActivityV2:
    settings = Inject(get_settings)
    if settings.FRACTAL_ENABLE_TASK_GROUP_RESET != "true":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "FRACTAL_ENABLE_TASK_GROUP_RESET="
                f"{settings.FRACTAL_ENABLE_TASK_GROUP_RESET}"
            ),
        )

    task_group = await _get_task_group_or_404(
        task_group_id=task_group_id,
        db=db,
    )
    if task_group.active is True:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                "Cannot re-collect an active task group. "
                "Please deactivate it first."
            ),
        )
    await check_no_ongoing_activity(task_group_id=task_group_id, db=db)
    owner = await db.get(UserOAuth, task_group.user_id)
    resource, profile = await validate_user_profile(user=owner, db=db)

    if (
        task_group.origin != TaskGroupOriginEnum.PYPI
        or resource.type == ResourceType.SLURM_SSH
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=(
                f"Not implemented ({task_group.origin=}, {resource.type=})."
            ),
        )

    logger.info(
        f"Running recollection for {task_group.id=} "
        f"({task_group.pkg_name} {task_group.version})"
    )
    if req_body.python_version is not None:
        get_python_interpreter_or_422(
            python_version=req_body.python_version,
            resource=resource,
        )
        logger.info(
            f"Replacing {task_group.python_version=} with "
            f"{req_body.python_version=}."
        )
        task_group.python_version = req_body.python_version
    if req_body.pip_extras is not None:
        logger.info(
            f"Replacing {task_group.pip_extras=} with {req_body.pip_extras=}."
        )
        task_group.pip_extras = req_body.pip_extras
    logger.info(
        f"Replacing {task_group.pinned_package_versions_pre=} with "
        f"{req_body.pinned_package_versions_pre=}"
    )
    task_group.pinned_package_versions_pre = (
        req_body.pinned_package_versions_pre
    )
    logger.info(
        f"Replacing {task_group.pinned_package_versions_post=} with "
        f"{req_body.pinned_package_versions_post=}"
    )
    task_group.pinned_package_versions_pre = (
        req_body.pinned_package_versions_post
    )
    db.add(task_group)
    await db.commit()

    task_group_activity = TaskGroupActivityV2(
        user_id=task_group.user_id,
        taskgroupv2_id=task_group.id,
        status=TaskGroupActivityStatus.PENDING,
        action=TaskGroupActivityAction.RESET,
        pkg_name=task_group.pkg_name,
        version=task_group.version,
        fractal_server_version=__VERSION__,
    )
    db.add(task_group_activity)
    await db.commit()
    await db.refresh(task_group_activity)

    background_tasks.add_task(
        reset_local,
        task_group_id=task_group.id,
        task_group_activity_id=task_group_activity.id,
        resource=resource,
        profile=profile,
        wheel_file=None,
    )

    logger.debug(
        "Task-collection endpoint: start background collection "
        "and return task_group_activity"
    )
    response.status_code = status.HTTP_202_ACCEPTED
    return task_group_activity
