from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import status
from packaging.version import parse
from packaging.version import Version
from pydantic import BaseModel
from pydantic import ConfigDict
from sqlmodel import cast
from sqlmodel import or_
from sqlmodel import select
from sqlmodel import String

from ....db import AsyncSession
from ....db import get_async_db
from ....models import LinkUserGroup
from ....models.v2 import TaskV2
from ._aux_functions import _get_workflow_check_owner
from ._aux_functions import _get_workflow_task_check_owner
from ._aux_functions_tasks import _check_type_filters_compatibility
from ._aux_functions_tasks import _get_task_group_or_404
from ._aux_functions_tasks import _get_task_read_access
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.routes.auth import current_active_user
from fractal_server.app.schemas.v2 import WorkflowTaskReadV2
from fractal_server.app.schemas.v2 import WorkflowTaskReplaceV2


router = APIRouter()


VALID_TYPE_UPDATES = {
    ("non_parallel", "converter_non_parallel"),
    ("compound", "converter_compound"),
    ("converter_non_parallel", "converter_non_parallel"),
    ("converter_compound", "converter_compound"),
    ("non_parallel", "non_parallel"),
    ("compound", "compound"),
    ("parallel", "parallel"),
}


def _is_type_update_valid(*, old_type: str, new_type: str) -> bool:
    return (old_type, new_type) in VALID_TYPE_UPDATES


def _is_version_parsable(version: str) -> bool:
    try:
        parse(version)
        return True
    except Exception:
        return False


class TaskVersion(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    task_id: int
    version: str
    parsed_version: Version


class TaskVersionRead(BaseModel):
    task_id: int
    version: str


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/version-update-candidates/"
)
async def get_workflow_version_update_candidates(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[list[TaskVersionRead]]:

    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    response = []
    for wftask in workflow.task_list:
        current_task = wftask.task

        # Skip tasks with no args schemas
        if not (
            current_task.args_schema_parallel
            or current_task.args_schema_non_parallel
        ):
            response.append([])
            continue

        current_task_group = await _get_task_group_or_404(
            task_group_id=current_task.taskgroupv2_id, db=db
        )

        # Skip tasks with non-parsable version
        if _is_version_parsable(current_task_group.version):
            current_parsed_version = parse(current_task_group.version)
        else:
            response.append([])
            continue

        res = await db.execute(
            select(TaskV2.id, TaskV2.type, TaskGroupV2.version)
            .where(
                or_(
                    cast(TaskV2.args_schema_parallel, String) != "null",
                    cast(TaskV2.args_schema_non_parallel, String) != "null",
                )
            )
            .where(TaskV2.name == current_task.name)
            .where(TaskV2.taskgroupv2_id == TaskGroupV2.id)
            .where(TaskGroupV2.pkg_name == current_task_group.pkg_name)
            .where(TaskGroupV2.active.is_(True))
            .where(
                or_(
                    TaskGroupV2.user_id == user.id,
                    TaskGroupV2.user_group_id.in_(
                        select(LinkUserGroup.group_id).where(
                            LinkUserGroup.user_id == user.id
                        )
                    ),
                )
            )
        )
        query_results: list[tuple[int, str, str]] = res.all()

        # Exclude tasks with non-compatible types or non-parsable versions
        current_task_type = current_task.type
        update_candidates = [
            TaskVersion(
                task_id=task_id,
                version=version,
                parsed_version=parse(version),
            )
            for task_id, _type, version in query_results
            if (
                _is_type_update_valid(
                    old_type=current_task_type,
                    new_type=_type,
                )
                and _is_version_parsable(version)
            )
        ]
        # Exclude tasks with old versions from update candidates
        update_candidates = [
            item
            for item in update_candidates
            if item.parsed_version > current_parsed_version
        ]
        # Sort update candidates by parsed version
        update_candidates = sorted(
            update_candidates,
            key=lambda obj: obj.parsed_version,
        )
        response.append(update_candidates)

    return response


@router.post(
    "/project/{project_id}/workflow/{workflow_id}/wftask/replace-task/",
    response_model=WorkflowTaskReadV2,
    status_code=status.HTTP_201_CREATED,
)
async def replace_workflowtask(
    project_id: int,
    workflow_id: int,
    workflow_task_id: int,
    task_id: int,
    replace: WorkflowTaskReplaceV2,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> WorkflowTaskReadV2:

    # Get objects from database
    old_wftask, workflow = await _get_workflow_task_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        workflow_task_id=workflow_task_id,
        user_id=user.id,
        db=db,
    )
    new_task = await _get_task_read_access(
        task_id=task_id,
        user_id=user.id,
        db=db,
        require_active=True,
    )

    # Preliminary checks
    if not _is_type_update_valid(
        old_type=old_wftask.task_type,
        new_type=new_task.type,
    ):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                "Cannot change task type from "
                f"{old_wftask.task_type} to {new_task.type}."
            ),
        )

    if replace.args_non_parallel is not None and new_task.type == "parallel":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot set 'args_non_parallel' for parallel task.",
        )
    if replace.args_parallel is not None and new_task.type == "non_parallel":
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Cannot set 'args_parallel' for non-parallel task.",
        )
    _check_type_filters_compatibility(
        task_input_types=new_task.input_types,
        wftask_type_filters=old_wftask.type_filters,
    )

    # Task arguments
    if replace.args_non_parallel is None:
        _args_non_parallel = old_wftask.args_non_parallel
    else:
        _args_non_parallel = replace.args_non_parallel
    if replace.args_parallel is None:
        _args_parallel = old_wftask.args_parallel
    else:
        _args_parallel = replace.args_parallel

    # If user's changes to `meta_non_parallel` are compatible with new task,
    # keep them; else, get `meta_non_parallel` from new task
    if (
        old_wftask.meta_non_parallel != old_wftask.task.meta_non_parallel
    ) and (old_wftask.task.meta_non_parallel == new_task.meta_non_parallel):
        _meta_non_parallel = old_wftask.meta_non_parallel
    else:
        _meta_non_parallel = new_task.meta_non_parallel
    # Same for `meta_parallel`
    if (old_wftask.meta_parallel != old_wftask.task.meta_parallel) and (
        old_wftask.task.meta_parallel == new_task.meta_parallel
    ):
        _meta_parallel = old_wftask.meta_parallel
    else:
        _meta_parallel = new_task.meta_parallel

    new_workflow_task = WorkflowTaskV2(
        task_id=new_task.id,
        task_type=new_task.type,
        task=new_task,
        # old-task values
        type_filters=old_wftask.type_filters,
        # possibly new values
        args_non_parallel=_args_non_parallel,
        args_parallel=_args_parallel,
        meta_non_parallel=_meta_non_parallel,
        meta_parallel=_meta_parallel,
    )

    workflow_task_order = old_wftask.order
    workflow.task_list.remove(old_wftask)
    workflow.task_list.insert(workflow_task_order, new_workflow_task)
    await db.commit()
    await db.refresh(new_workflow_task)
    return new_workflow_task
