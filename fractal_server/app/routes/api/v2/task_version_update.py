from functools import total_ordering

from fastapi import APIRouter
from fastapi import Depends
from packaging.version import parse
from pydantic import BaseModel
from pydantic import field_validator
from sqlmodel import cast
from sqlmodel import or_
from sqlmodel import select
from sqlmodel import String

from ....db import AsyncSession
from ....db import get_async_db
from ....models import LinkUserGroup
from ....models.v2 import TaskV2
from ._aux_functions import _get_workflow_check_owner
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.auth import current_active_user

router = APIRouter()


@total_ordering
class TaskVersion(BaseModel):
    task_id: int
    version: str

    @field_validator("version")
    @classmethod
    def validate_version(cls, v: str) -> str:
        parse(v)
        return v

    def __eq__(self, other):
        return parse(self.version) == parse(other.version)

    def __lt__(self, other):
        return parse(self.version) < parse(other.version)


@router.get(
    "/project/{project_id}/workflow/{workflow_id}/version-update-candidates/"
)
async def get_workflow_version_update_candidates(
    project_id: int,
    workflow_id: int,
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
) -> list[list[TaskVersion]]:

    workflow = await _get_workflow_check_owner(
        project_id=project_id,
        workflow_id=workflow_id,
        user_id=user.id,
        db=db,
    )

    response = []
    for wftask in workflow.task_list:
        task = wftask.task
        if not (task.args_schema_parallel or task.args_schema_non_parallel):
            response.append([])
            continue
        current_task_group = await db.get(TaskGroupV2, task.taskgroupv2_id)

        res = await db.execute(
            select(TaskV2.id, TaskGroupV2.version)
            .where(
                or_(
                    cast(TaskV2.args_schema_parallel, String) != "null",
                    cast(TaskV2.args_schema_non_parallel, String) != "null",
                )
            )
            .where(TaskV2.name == task.name)
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
        query_results: list[tuple[int, str]] = res.all()
        task_version = sorted(
            [
                TaskVersion(task_id=task_id, version=version)
                for task_id, version in query_results
            ]
        )
        version_threshold = TaskVersion(
            task_id=0,  # irrelevant
            version=current_task_group.version,
        )
        filtered_groups_and_task_ids = [
            item for item in task_version if item > version_threshold
        ]
        response.append(filtered_groups_and_task_ids)

    return response
