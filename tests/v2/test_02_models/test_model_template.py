import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models.security import UserGroup
from fractal_server.app.models.v2 import WorkflowTemplate
from fractal_server.app.models.v2 import WorkflowV2


async def test_workflow_template(
    db, MockCurrentUser, user_group_factory, project_factory
):
    # Setup
    async with MockCurrentUser() as user:
        user_id = user.id
        project = await project_factory(user)
        project_id = project.id

    user_group = await user_group_factory("group", user_id, db=db)
    user_group_id = user_group.id

    # Test mandatory args

    mandatory_args = dict(
        user_id=user_id,
        name="template",
        version=1,
        data={},
    )
    for missing in mandatory_args:
        db.add(
            WorkflowTemplate(
                **{
                    arg: value
                    for arg, value in mandatory_args.items()
                    if arg != missing
                }
            )
        )
        with pytest.raises(IntegrityError, match="NotNullViolation"):
            await db.commit()
        await db.rollback()

    template1 = WorkflowTemplate(**mandatory_args)
    db.add(template1)
    await db.commit()
    await db.refresh(template1)
    template1_id = template1.id

    # Test UserGroup foreign key

    template2 = WorkflowTemplate(
        user_id=user_id,
        name="template 2",
        version=1,
        data={},
        user_group_id=user_group_id,
    )
    db.add(template2)
    await db.commit()

    await db.refresh(template2)
    assert template2.user_group_id == user_group_id

    user_group = await db.get(UserGroup, user_group_id)
    await db.delete(user_group)
    await db.commit()

    await db.refresh(template2)
    assert template2.user_group_id is None

    # Test ix_workflowtemplate_user_name_version_unique_constraint

    template3 = WorkflowTemplate(**mandatory_args)
    db.add(template3)
    with pytest.raises(
        IntegrityError,
        match="ix_workflowtemplate_user_name_version_unique_constraint",
    ):
        await db.commit()
    await db.rollback()

    # Test Workflow.template_id

    workflow = WorkflowV2(
        name="workflow",
        project_id=project_id,
        template_id=template1_id,
    )
    db.add(workflow)
    await db.commit()

    await db.refresh(workflow)
    assert workflow.template_id == template1_id

    template1 = await db.get(WorkflowTemplate, template1_id)
    await db.delete(template1)
    await db.commit()

    await db.refresh(workflow)
    assert workflow.template_id is None
