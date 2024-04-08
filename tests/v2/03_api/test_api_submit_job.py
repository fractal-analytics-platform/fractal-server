import time

import pytest
from devtools import debug

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.api.v2.submit import _encode_as_utc
from fractal_server.app.schemas.v2.dumps import DatasetDumpV2
from fractal_server.app.schemas.v2.dumps import ProjectDumpV2
from fractal_server.app.schemas.v2.dumps import WorkflowDumpV2

PREFIX = "/api/v2"


async def test_submit_job_failures_non_verified_user(
    client,
    MockCurrentUser,
):
    """
    Test that non-verified users are not authorized to make calls
    to `/api/v1/project/123/workflow/123/apply/`.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(
            f"{PREFIX}/project/123/job/submit/"
            "?workflow_id=123&dataset_id=123",
            json={},
        )
        assert res.status_code == 401


async def test_submit_job_failures(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project1 = await project_factory_v2(user)
        project2 = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project1.id, name="dataset"
        )
        dataset_read_only = await dataset_factory_v2(
            project_id=project1.id, name="dataset-read-only", read_only=True
        )

        workflow1 = await workflow_factory_v2(project_id=project1.id)
        workflow2 = await workflow_factory_v2(project_id=project1.id)
        workflow3 = await workflow_factory_v2(project_id=project2.id)

        task = await task_factory_v2()
        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )

        # (A) Not existing workflow
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id=123&dataset_id={dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # (B) Workflow with wrong project_id
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id={workflow3.id}&dataset_id={dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422

        # (C) Not existing dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id={workflow1.id}&dataset_id=999999999",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # (D) Read-only dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id={workflow1.id}&dataset_id={dataset_read_only.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "read_only" in res.json()["detail"]

        # (E) Workflow without tasks
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id={workflow2.id}&dataset_id={dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "empty task list" in res.json()["detail"]


async def test_submit_jobs_with_same_dataset(
    db,
    client,
    project_factory_v2,
    job_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    tmp_path,
    MockCurrentUser,
):
    """
    Test behavior for when another job with the same output_dataset_id already
    exists.
    """

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory_v2(user)
        dataset1 = await dataset_factory_v2(
            project_id=project.id, name="dataset1"
        )
        dataset2 = await dataset_factory_v2(
            project_id=project.id, name="dataset2"
        )
        new_task = await task_factory_v2()
        workflow = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=new_task.id, db=db
        )

        # Existing jobs with done/running status
        existing_job_A_done = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset1.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="done",
        )
        debug(existing_job_A_done)
        existing_job_B_done = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset2.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="submitted",
        )
        debug(existing_job_B_done)

        # API call succeeds when the other job with the same dataset has
        # status="done"
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 202

        # API call fails when the other job with the same output_dataset has
        # status="done"
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset2.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert (
            f"Dataset {dataset2.id} is already in use" in res.json()["detail"]
        )


async def test_project_apply_missing_user_attributes(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    override_settings_factory,
):
    """
    When using the slurm backend, user.slurm_user and user.cache_dir become
    required attributes. If they are missing, the apply endpoint fails with a
    422 error.
    """

    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        # Make sure that user.cache_dir was not set
        debug(user)
        assert user.cache_dir is None

        # Create project, datasets, workflow, task, workflowtask
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id, name="ds")

        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2()
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        # Call apply endpoint
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "user.cache_dir=None" in res.json()["detail"]

        user.cache_dir = "/tmp"
        user.slurm_user = None
        await db.commit()

        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "user.slurm_user=None" in res.json()["detail"]


async def test_project_apply_workflow_subset(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory_v2(user)
        dataset1 = await dataset_factory_v2(
            project_id=project.id, name="ds1", type="type1"
        )
        dataset2 = await dataset_factory_v2(
            project_id=project.id, name="ds2", type="type2"
        )

        workflow = await workflow_factory_v2(project_id=project.id)

        task12 = await task_factory_v2(source="admin:1to2")
        task23 = await task_factory_v2(source="admin:2to3")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task12.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task23.id, db=db
        )

        debug(workflow)

        # This job (with no first_task_index or last_task_index) is submitted
        # correctly (and then fails, because tasks have invalid `command`
        # values)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json={},
        )
        debug(res.json())
        job_id = res.json()["id"]
        assert res.status_code == 202
        res = await client.get(f"{PREFIX}/project/{project.id}/job/{job_id}/")
        assert res.json()["status"] == "failed"

        # These two jobs (with valid first_task_index and last_task_index) are
        # submitted correctly (and then fail)
        # Case A
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=0, last_task_index=0),
        )
        debug(res.json())
        job_id = res.json()["id"]
        assert res.status_code == 202
        res = await client.get(f"{PREFIX}/project/{project.id}/job/{job_id}/")
        assert res.json()["status"] == "failed"

        # Wait, to avoid RuntimeError: Workflow dir ... already exists.
        time.sleep(1)

        # Case B
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset2.id}",
            json=dict(first_task_index=1, last_task_index=1),
        )
        debug(res.json())
        job_id = res.json()["id"]
        assert res.status_code == 202
        res = await client.get(f"{PREFIX}/project/{project.id}/job/{job_id}/")
        assert res.json()["status"] == "failed"

        # Jobs with invalid first_task_index and last_task_index are not
        # submitted

        # Case B (invalid first_task_index)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=-2, last_task_index=1),
        )
        debug(res.json())
        assert res.status_code == 422

        # Case C (invalid last_task_index)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=0, last_task_index=99),
        )
        debug(res.json())
        assert res.status_code == 422

        # Case D (first_task_index and last_task_index exchanged)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=1, last_task_index=0),
        )
        debug(res.json())
        assert res.status_code == 422

        # Check dumps field
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=0, last_task_index=1),
        )
        expected_project_dump = ProjectDumpV2(
            **project.model_dump(exclude={"user_list", "timestamp_created"}),
            timestamp_created=_encode_as_utc(project.timestamp_created),
        ).dict()
        expected_workflow_dump = WorkflowDumpV2(
            **workflow.model_dump(exclude={"task_list", "timestamp_created"}),
            timestamp_created=_encode_as_utc(workflow.timestamp_created),
        ).dict()
        expected_dataset_dump = DatasetDumpV2(
            **dataset1.model_dump(exclude={"timestamp_created"}),
            timestamp_created=_encode_as_utc(dataset1.timestamp_created),
        ).dict()
        assert res.json()["project_dump"] == expected_project_dump
        assert res.json()["workflow_dump"] == expected_workflow_dump
        assert res.json()["dataset_dump"] == expected_dataset_dump


async def test_project_apply_slurm_account(
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    client,
    db,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="ds1", type="type1"
        )
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(source="source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        # User has an empty SLURM accounts list
        assert user.slurm_accounts == []

        # If no slurm_account is provided, it's automatically set to None
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202
        assert res.json()["slurm_account"] is None

        # If a slurm_account is provided, we get a 422
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json=dict(slurm_account="NOT IN THE LIST"),
        )
        assert res.status_code == 422

    SLURM_LIST = ["foo", "bar", "rab", "oof"]
    async with MockCurrentUser(
        user_kwargs={"slurm_accounts": SLURM_LIST, "is_verified": True}
    ) as user2:
        project = await project_factory_v2(user2)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="ds2", type="type2"
        )
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(
            input_type="type2",
            output_type="type2",
            source="source2",
            command="ls",
        )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        # User has a non empty SLURM accounts list
        assert user2.slurm_accounts == SLURM_LIST

        # If no slurm_account is provided, we use the first one of the list

        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202
        assert res.json()["slurm_account"] == SLURM_LIST[0]

        # If a slurm_account from the list is provided, we use it
        for account in SLURM_LIST:
            res = await client.post(
                f"{PREFIX}/project/{project.id}/job/submit/"
                f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
                json=dict(slurm_account=account),
            )
            debug(res.json())
            assert res.status_code == 202
            assert res.json()["slurm_account"] == account

        # If a slurm_account outside the list is provided, we get a 422
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json=dict(slurm_account="NOT IN THE LIST"),
        )
        assert res.status_code == 422


async def test_rate_limit(
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    client,
    db,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_API_SUBMIT_RATE_LIMIT=1)
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id, name="ds")
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(source="source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        # Call 1: OK
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202
        time.sleep(1)
        # Call 2: OK
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202
        # Call 2: too early!
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 429
        assert "less than 1 second" in res.json()["detail"]
        time.sleep(1)
        # Call 3: OK
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202
