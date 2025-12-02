import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2.job import JobStatusType


async def test_unique_job_submitted_per_dataset(db, local_resource_profile_db):
    resource, _ = local_resource_profile_db

    project = ProjectV2(name="Project", resource_id=resource.id)
    db.add(project)
    await db.commit()
    await db.refresh(project)

    workflow = WorkflowV2(name="Workflow", project_id=project.id)
    dataset1 = DatasetV2(
        name="Dataset1", project_id=project.id, zarr_dir="/fake"
    )
    dataset2 = DatasetV2(
        name="Dataset2", project_id=project.id, zarr_dir="/fake"
    )
    db.add_all([workflow, dataset1, dataset2])
    await db.commit()
    await db.refresh(workflow)
    await db.refresh(dataset1)
    await db.refresh(dataset2)

    dataset1_id = dataset1.id
    dataset2_id = dataset2.id

    common_args = dict(
        project_id=project.id,
        workflow_id=workflow.id,
        user_email="user@example.org",
        dataset_dump={},
        workflow_dump={},
        project_dump={},
        first_task_index=0,
        last_task_index=0,
        attribute_filters={},
        type_filters={},
    )

    # Dataset 1, SUBMITTED -> OK
    db.add(
        JobV2(
            dataset_id=dataset1_id,
            status=JobStatusType.SUBMITTED,
            **common_args,
        )
    )
    await db.commit()

    # Dataset 1, NON SUBMITTED -> OK
    db.add(
        JobV2(
            dataset_id=dataset1_id,
            status=JobStatusType.FAILED,
            **common_args,
        )
    )
    await db.commit()

    # Dataset 1, SUBMITTED -> FAIL
    db.add(
        JobV2(
            dataset_id=dataset1_id,
            status=JobStatusType.SUBMITTED,
            **common_args,
        )
    )
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    assert "ix_jobv2_one_submitted_job_per_dataset" in e.value.args[0]
    await db.rollback()

    # Dataset 2, SUBMITTED -> OK
    db.add(
        JobV2(
            dataset_id=dataset2_id,
            status=JobStatusType.SUBMITTED,
            **common_args,
        )
    )
    await db.commit()

    # NOTE: the following tests a situation that should never happens,
    # i.e. dataset_id=None, status="submitted"

    # Dataset NULL, SUBMITTED -> OK
    db.add(
        JobV2(
            dataset_id=None,
            status=JobStatusType.SUBMITTED,
            **common_args,
        )
    )
    await db.commit()

    # Dataset NULL, SUBMITTED -> OK
    db.add(
        JobV2(
            dataset_id=None,
            status=JobStatusType.SUBMITTED,
            **common_args,
        )
    )
    await db.commit()
