import pytest
from fastapi import HTTPException

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.routes.api.v2._aux_functions_task_lifecycle import (
    check_no_related_workflowtask,
)
from fractal_server.app.schemas.v2 import TaskGroupOriginEnum


async def test_check_no_related_workflowtask(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(user_kwargs={"profile_id": profile.id}) as user:
        task1 = TaskV2(name="task1", type="parallel", command_parallel="cmd")
        task2 = TaskV2(
            name="task2", type="non_parallel", command_non_parallel="cmd"
        )
        task_group = TaskGroupV2(
            user_id=user.id,
            origin=TaskGroupOriginEnum.OTHER,
            pkg_name="pkg",
            task_list=[task1, task2],
            resource_id=resource.id,
        )
        db.add(task_group)
        await db.commit()

        await check_no_related_workflowtask(task_group=task_group, db=db)

        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)

        await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task_group.task_list[-1].id
        )

        with pytest.raises(
            HTTPException, match=f"TaskV2 {task2.id} is still in use"
        ):
            await check_no_related_workflowtask(task_group=task_group, db=db)
