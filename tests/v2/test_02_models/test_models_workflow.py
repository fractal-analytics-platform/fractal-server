import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2


async def test_project_and_workflows(db, local_resource_profile_db):
    with pytest.raises(IntegrityError):
        # missing relatioship with project
        broken_workflow = WorkflowV2(name="broken")
        db.add(broken_workflow)
        await db.commit()
    await db.rollback()

    resource, profile = local_resource_profile_db

    project = ProjectV2(name="project", resource_id=resource.id)

    # using `.project` relationship
    workflow1 = WorkflowV2(name="workflow1", project=project)
    db.add(project)
    db.add(workflow1)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(ProjectV2))
    db_project = project_query.scalars().one()
    workflow_query = await db.execute(select(WorkflowV2))
    db_workflow1 = workflow_query.scalars().one()

    # test relationships
    assert db_workflow1.project_id == db_project.id
    assert db_workflow1.project.model_dump() == db_project.model_dump()
    # test defaults
    assert db_workflow1.task_list == []

    # using `.project_id` attribute
    workflow2 = WorkflowV2(name="workflow2", project_id=db_project.id)
    db.add(workflow2)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(ProjectV2))
    db_project = project_query.scalars().one()

    workflow_query = await db.execute(select(WorkflowV2))
    db_workflow1, db_workflow2 = workflow_query.scalars().all()

    # test relationships
    assert db_workflow1.name == workflow1.name
    assert db_workflow2.name == workflow2.name
    assert db_workflow2.project_id == db_project.id
    assert db_workflow2.project.model_dump() == db_project.model_dump()

    # delete just one workflow
    await db.delete(db_workflow2)

    workflow_query = await db.execute(select(WorkflowV2))
    db_workflow = workflow_query.scalars().one()
    assert db_workflow.name == workflow1.name

    # delete the project
    project_query = await db.execute(select(ProjectV2))
    db_project = project_query.scalars().one()
    await db.delete(db_project)

    await db.commit()
    db.expunge_all()

    workflow_query = await db.execute(select(WorkflowV2))
    db_workflow = workflow_query.scalars().one_or_none()
    assert db_workflow is None
