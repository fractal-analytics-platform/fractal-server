import json
import time

import pytest
from devtools import debug

from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v1.dumps import DatasetDumpV1
from fractal_server.app.schemas.v1.dumps import ProjectDumpV1
from fractal_server.app.schemas.v1.dumps import WorkflowDumpV1

PREFIX = "/api/v1"


async def test_project_apply_failures_non_verified_user(
    client,
    MockCurrentUser,
):
    """
    Test that non-verified users are not authorized to make calls
    to `/api/v1/project/123/workflow/123/apply/`.
    """
    async with MockCurrentUser(user_kwargs=dict(is_verified=False)):
        res = await client.post(
            f"{PREFIX}/project/123/workflow/123/apply/"
            f"?input_dataset_id=123"
            f"&output_dataset_id=123",
            json={},
        )
        assert res.status_code == 401


async def test_project_apply_failures(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project1 = await project_factory(user)
        project2 = await project_factory(user)
        input_dataset = await dataset_factory(
            project_id=project1.id, name="input"
        )
        output_dataset = await dataset_factory(
            project_id=project1.id, name="output"
        )
        output_dataset_read_only = await dataset_factory(
            project_id=project1.id, name="output", read_only=True
        )
        output_dataset_wrong_type = await dataset_factory(
            project_id=project1.id, name="output", type="invalid_type"
        )
        output_dataset_two_resources = await dataset_factory(
            project_id=project1.id, name="output"
        )

        await resource_factory(input_dataset)
        await resource_factory(output_dataset)
        await resource_factory(output_dataset_read_only)
        await resource_factory(output_dataset_wrong_type)
        await resource_factory(output_dataset_two_resources)
        await resource_factory(output_dataset_two_resources)

        workflow1 = await workflow_factory(project_id=project1.id)
        workflow2 = await workflow_factory(project_id=project1.id)
        workflow3 = await workflow_factory(project_id=project2.id)

        task = await task_factory()
        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )

        # (A) Not existing workflow
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/123/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # (B) Workflow with wrong project_id
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow3.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422

        # (C) Not existing output dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}&output_dataset_id=123",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # (D) Missing output_dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422

        # (E) Read-only output_dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_read_only.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "read_only" in res.json()["detail"]

        # (F) output_dataset with wrong type
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_wrong_type.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "Incompatible types" in res.json()["detail"]

        # (G) output_dataset with two resources
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_two_resources.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "must have a single resource" in res.json()["detail"]

        # (H) Workflow without tasks
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow2.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "empty task list" in res.json()["detail"]


async def test_project_apply_existing_job(
    db,
    client,
    project_factory,
    job_factory,
    workflow_factory,
    dataset_factory,
    resource_factory,
    task_factory,
    tmp_path,
    MockCurrentUser,
):
    """
    Test behavior for when another job with the same output_dataset_id already
    exists.
    """

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project_id=project.id, name="input"
        )
        output_dataset_A = await dataset_factory(
            project_id=project.id, name="output-A"
        )
        output_dataset_B = await dataset_factory(
            project_id=project.id, name="output-B"
        )
        await resource_factory(input_dataset)
        await resource_factory(output_dataset_A)
        await resource_factory(output_dataset_B)

        new_task = await task_factory(
            input_type="Any",
            output_type="Any",
        )
        workflow = await workflow_factory(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=new_task.id, db=db
        )

        # Existing jobs with done/running status
        existing_job_A_done = await job_factory(
            project_id=project.id,
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset_A.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="done",
        )
        debug(existing_job_A_done)
        existing_job_B_done = await job_factory(
            project_id=project.id,
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset_B.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="submitted",
        )
        debug(existing_job_B_done)

        # API call succeeds when the other job with the same output_dataset has
        # status="done"
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_A.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 202

        # API call fails when the other job with the same output_dataset has
        # status="done"
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_B.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert (
            f"Output dataset {output_dataset_B.id} is already in use"
            in res.json()["detail"]
        )


async def test_project_apply_missing_user_attributes(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
    override_settings_factory,
):
    """
    When using the slurm backend, `user.settings.slurm_user` and
    `user.settings.project_dir` become required attributes.
    If they are missing, the apply endpoint fails with a 422 error.
    """

    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")

    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        # Make sure that user.settings.project_dir was not set
        debug(user)
        assert user.settings.project_dir is None

        # Create project, datasets, workflow, task, workflowtask
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project_id=project.id, name="input", type="zarr"
        )
        output_dataset = await dataset_factory(
            project_id=project.id, name="output"
        )
        for dataset_id in [input_dataset.id, output_dataset.id]:
            res = await client.post(
                f"{PREFIX}/project/{project.id}/"
                f"dataset/{dataset_id}/resource/",
                json=dict(path="/some/absolute/path"),
            )
            assert res.status_code == 201
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(input_type="zarr")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        # Call apply endpoint
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "User settings are not valid" in res.json()["detail"]

        user.settings.project_dir = "/tmp"
        user.settings.slurm_user = None
        await db.merge(user)
        await db.commit()

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "User settings are not valid" in res.json()["detail"]


async def test_project_apply_missing_resources(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    db,
    client,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project_id=project.id, name="input", type="zarr"
        )
        output_dataset = await dataset_factory(
            project_id=project.id, name="output"
        )
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory()
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        debug(input_dataset)
        debug(output_dataset)

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )

        debug(res.json())
        assert res.status_code == 422
        assert "empty resource_list" in res.json()["detail"]


async def test_project_apply_workflow_subset(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory(user)
        dataset1 = await dataset_factory(
            project_id=project.id, name="ds1", type="type1"
        )
        dataset2 = await dataset_factory(
            project_id=project.id, name="ds2", type="type2"
        )
        dataset3 = await dataset_factory(
            project_id=project.id, name="ds3", type="type3"
        )

        await resource_factory(dataset1)
        await resource_factory(dataset2)
        await resource_factory(dataset3)

        workflow = await workflow_factory(project_id=project.id)

        task12 = await task_factory(
            input_type="type1", output_type="type2", source="admin:1to2"
        )
        task23 = await task_factory(
            input_type="type2", output_type="type3", source="admin:2to3"
        )
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
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset1.id}"
            f"&output_dataset_id={dataset3.id}",
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
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset1.id}"
            f"&output_dataset_id={dataset2.id}",
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
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset2.id}"
            f"&output_dataset_id={dataset3.id}",
            json=dict(first_task_index=1, last_task_index=1),
        )
        debug(res.json())
        job_id = res.json()["id"]
        assert res.status_code == 202
        res = await client.get(f"{PREFIX}/project/{project.id}/job/{job_id}/")
        assert res.json()["status"] == "failed"

        # Jobs with invalid first_task_index and last_task_index are not
        # submitted

        # Case A (type mismatch for workflow subset)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset1.id}"
            f"&output_dataset_id={dataset3.id}",
            json=dict(first_task_index=0, last_task_index=0),
        )
        debug(res.json())
        assert res.status_code == 422

        # Case B (invalid first_task_index)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset1.id}"
            f"&output_dataset_id={dataset3.id}",
            json=dict(first_task_index=-2, last_task_index=1),
        )
        debug(res.json())
        assert res.status_code == 422

        # Case C (invalid last_task_index)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset1.id}"
            f"&output_dataset_id={dataset3.id}",
            json=dict(first_task_index=0, last_task_index=99),
        )
        debug(res.json())
        assert res.status_code == 422

        # Case D (first_task_index and last_task_index exchanged)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset1.id}"
            f"&output_dataset_id={dataset3.id}",
            json=dict(first_task_index=1, last_task_index=0),
        )
        debug(res.json())
        assert res.status_code == 422

        # Check dumps field
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset1.id}"
            f"&output_dataset_id={dataset3.id}",
            json=dict(first_task_index=0, last_task_index=1),
        )
        expected_project_dump = ProjectDumpV1(
            **json.loads(project.json(exclude={"user_list"})),
        ).dict()
        expected_workflow_dump = WorkflowDumpV1(
            **json.loads(workflow.json(exclude={"task_list"})),
        ).dict()
        expected_input_dataset_dump = DatasetDumpV1(
            **json.loads(dataset1.json()),
            resource_list=[
                resource.model_dump() for resource in dataset1.resource_list
            ],
        ).dict()
        expected_output_dataset_dump = DatasetDumpV1(
            **json.loads(dataset3.json()),
            resource_list=[
                resource.model_dump() for resource in dataset3.resource_list
            ],
        ).dict()
        assert res.json()["project_dump"] == expected_project_dump
        assert res.json()["workflow_dump"] == expected_workflow_dump
        assert res.json()["input_dataset_dump"] == expected_input_dataset_dump
        assert (
            res.json()["output_dataset_dump"] == expected_output_dataset_dump
        )


async def test_project_apply_slurm_account(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
    client,
    db,
):
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(
            project_id=project.id, name="ds1", type="type1"
        )

        await resource_factory(dataset)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(
            input_type="type1",
            output_type="type1",
            source="source",
            command="ls",
        )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        # User has an empty SLURM accounts list
        assert user.settings.slurm_accounts == []

        # If no slurm_account is provided, it's automatically set to None
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}&output_dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202
        assert res.json()["slurm_account"] is None

        # If a slurm_account is provided, we get a 422
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}&output_dataset_id={dataset.id}",
            json=dict(slurm_account="NOT IN THE LIST"),
        )
        assert res.status_code == 422

    SLURM_LIST = ["foo", "bar", "rab", "oof"]
    async with MockCurrentUser(
        user_kwargs={"is_verified": True},
        user_settings_dict={"slurm_accounts": SLURM_LIST},
    ) as user2:
        project = await project_factory(user2)
        dataset = await dataset_factory(
            project_id=project.id, name="ds2", type="type2"
        )
        await resource_factory(dataset)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(
            input_type="type2",
            output_type="type2",
            source="source2",
            command="ls",
        )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        # User has a non empty SLURM accounts list
        assert user2.settings.slurm_accounts == SLURM_LIST

        # If no slurm_account is provided, we use the first one of the list

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}&output_dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202
        assert res.json()["slurm_account"] == SLURM_LIST[0]

        # If a slurm_account from the list is provided, we use it
        for account in SLURM_LIST:
            res = await client.post(
                f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
                f"?input_dataset_id={dataset.id}"
                f"&output_dataset_id={dataset.id}",
                json=dict(slurm_account=account),
            )
            debug(res.json())
            assert res.status_code == 202
            assert res.json()["slurm_account"] == account

        # If a slurm_account outside the list is provided, we get a 422
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}&output_dataset_id={dataset.id}",
            json=dict(slurm_account="NOT IN THE LIST"),
        )
        assert res.status_code == 422


@pytest.mark.skip(reason="Flaky")
async def test_rate_limit(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
    client,
    db,
    override_settings_factory,
    tmp_path,
):
    override_settings_factory(
        FRACTAL_API_SUBMIT_RATE_LIMIT=1,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp_path / "artifacts",
    )
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:

        project = await project_factory(user)
        dataset_i = await dataset_factory(
            project_id=project.id, name="ds1", type="type"
        )
        dataset_o = await dataset_factory(
            project_id=project.id, name="ds2", type="type"
        )
        await resource_factory(dataset_i)
        await resource_factory(dataset_o)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(
            input_type="type",
            output_type="type",
            source="source",
            command="ls",
        )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        # Call 1: OK
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset_i.id}"
            f"&output_dataset_id={dataset_o.id}",
            json={},
        )
        assert res.status_code == 202
        time.sleep(1)
        # Call 2: OK
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset_i.id}"
            f"&output_dataset_id={dataset_o.id}",
            json={},
        )
        assert res.status_code == 202
        # Call 2: too early!
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset_i.id}"
            f"&output_dataset_id={dataset_o.id}",
            json={},
        )
        assert res.status_code == 429
        assert "less than 1 second" in res.json()["detail"]
        time.sleep(1)
        # Call 3: OK
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset_i.id}"
            f"&output_dataset_id={dataset_o.id}",
            json={},
        )
        assert res.status_code == 202
