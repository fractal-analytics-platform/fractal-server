import json
import time
from datetime import timedelta

import pytest
from devtools import debug

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.app.schemas.v2.dumps import DatasetDumpV2
from fractal_server.app.schemas.v2.dumps import ProjectDumpV2
from fractal_server.app.schemas.v2.dumps import WorkflowDumpV2
from fractal_server.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.runner.filenames import WORKFLOW_LOG_FILENAME

PREFIX = "/api/v2"
backends_available = list(element.value for element in ResourceType)


async def test_submit_job_failures_non_verified_user(
    client,
    MockCurrentUser,
):
    """
    Test that non-verified users are not authorized to make calls
    to `/api/v2/project/123/workflow/123/apply/`.
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
    local_resource_profile_db,
    slurm_sudo_resource_profile_db,
):
    res, prof = local_resource_profile_db
    res2, _ = slurm_sudo_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        task = await task_factory_v2(user_id=user.id)
        # 1
        project1 = await project_factory_v2(user)
        dataset1 = await dataset_factory_v2(
            project_id=project1.id, name="dataset1"
        )
        workflow1a = await workflow_factory_v2(project_id=project1.id)
        workflow1b = await workflow_factory_v2(project_id=project1.id)
        await _workflow_insert_task(
            workflow_id=workflow1a.id, task_id=task.id, db=db
        )
        # 2
        project2 = await project_factory_v2(user)
        workflow2 = await workflow_factory_v2(project_id=project2.id)
        # 3
        project3 = await project_factory_v2(user, resource_id=res2.id)
        dataset3 = await dataset_factory_v2(
            project_id=project3.id, name="dataset3"
        )
        workflow3 = await workflow_factory_v2(project_id=project3.id)
        await _workflow_insert_task(
            workflow_id=workflow3.id, task_id=task.id, db=db
        )

        # (A) Not existing workflow
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id=123&dataset_id={dataset1.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # (B) Workflow with wrong project_id
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id={workflow2.id}&dataset_id={dataset1.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422

        # (C) Not existing dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id={workflow1a.id}&dataset_id=999999999",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # (D) Workflow without tasks
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/job/submit/"
            f"?workflow_id={workflow1b.id}&dataset_id={dataset1.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "empty task list" in res.json()["detail"]

        # (E) Project resource different from user's resource
        res = await client.post(
            f"{PREFIX}/project/{project3.id}/job/submit/"
            f"?workflow_id={workflow3.id}&dataset_id={dataset3.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Project resource does not match with user's resource"
        )


async def test_submit_job_ssh_connection_failure(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    tmp777_path,
    slurm_ssh_resource_profile_fake_db,
):
    resource, prof = slurm_ssh_resource_profile_fake_db

    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="ds1", type="type1"
        )
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id, name="1to2")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        res = await client.post(
            f"/api/v2/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 422
        assert (
            res.json()["detail"] == "Error in setting up the SSH connection."
        )


async def test_submit_incompatible_filters(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    local_resource_profile_db,
):
    res, prof = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        task = await task_factory_v2(user_id=user.id, input_types={"a": True})

        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)

        workflow1 = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            db=db,
            workflow_id=workflow1.id,
            task_id=task.id,
            type_filters={"a": False},
        )

        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow1.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 422
        assert "filters" in res.json()["detail"]

        workflow2 = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            db=db,
            workflow_id=workflow2.id,
            task_id=task.id,
            type_filters={"a": True},
        )
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow2.id}&dataset_id={dataset.id}",
            json={},
        )
        assert res.status_code == 202


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
    local_resource_profile_db,
):
    """
    Test behavior for when another job with the same output_dataset_id already
    exists.
    """
    res, prof = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        project = await project_factory_v2(user)
        dataset1 = await dataset_factory_v2(
            project_id=project.id, name="dataset1"
        )
        dataset2 = await dataset_factory_v2(
            project_id=project.id, name="dataset2"
        )
        new_task = await task_factory_v2(user_id=user.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=new_task.id, db=db
        )

        # Existing jobs with done/running status
        await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset1.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="done",
        )
        await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset2.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="submitted",
        )

        # API call succeeds when the other job with the same dataset has
        # status="done"
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset1.id}",
            json={},
        )
        assert res.status_code == 202

        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{res.json()['id']}/download/"
        )
        assert res.status_code == 200

        # API call fails when the other job with the same output_dataset has
        # status="done"
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset2.id}",
            json={},
        )
        assert res.status_code == 422
        assert (
            f"Dataset {dataset2.id} is already in use" in res.json()["detail"]
        )


async def test_project_apply_workflow_subset(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    local_resource_profile_db,
):
    res, prof = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        project = await project_factory_v2(user)
        dataset1 = await dataset_factory_v2(
            project_id=project.id, name="ds1", type="type1"
        )
        dataset2 = await dataset_factory_v2(
            project_id=project.id, name="ds2", type="type2"
        )

        wf = await workflow_factory_v2(project_id=project.id)

        task12 = await task_factory_v2(user_id=user.id, name="1to2")
        task23 = await task_factory_v2(user_id=user.id, name="2to3")
        await _workflow_insert_task(
            workflow_id=wf.id, task_id=task12.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=wf.id, task_id=task23.id, db=db
        )

        # This job (with no first_task_index or last_task_index) is submitted
        # correctly (and then fails, because tasks have invalid `command`
        # values)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={wf.id}&dataset_id={dataset1.id}",
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
            f"?workflow_id={wf.id}&dataset_id={dataset1.id}",
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
            f"?workflow_id={wf.id}&dataset_id={dataset2.id}",
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
            f"?workflow_id={wf.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=-2, last_task_index=1),
        )
        debug(res.json())
        assert res.status_code == 422

        # Case C (invalid last_task_index)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={wf.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=0, last_task_index=99),
        )
        debug(res.json())
        assert res.status_code == 422

        # Case D (first_task_index and last_task_index exchanged)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={wf.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=1, last_task_index=0),
        )
        debug(res.json())
        assert res.status_code == 422

        # Check dumps field
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={wf.id}&dataset_id={dataset1.id}",
            json=dict(first_task_index=0, last_task_index=1),
        )
        expected_project_dump = ProjectDumpV2(
            **json.loads(
                project.model_dump_json(exclude={"user_list", "resource_id"})
            )
        ).model_dump()
        expected_workflow_dump = WorkflowDumpV2(
            **json.loads(wf.model_dump_json(exclude={"task_list"}))
        ).model_dump()
        expected_dataset_dump = DatasetDumpV2(
            **json.loads(
                dataset1.model_dump_json(exclude={"history", "images"})
            )
        ).model_dump()
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
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=profile.id,
        )
    ) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="ds1", type="type1"
        )
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)
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
        user_kwargs={
            "is_verified": True,
            "profile_id": profile.id,
            "slurm_accounts": SLURM_LIST,
        },
    ) as user2:
        project = await project_factory_v2(user2)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="ds2", type="type2"
        )
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(
            user_id=user2.id,
            input_type="type2",
            output_type="type2",
            command="ls",
            name="ls",
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


async def test_get_jobs(
    db,
    client,
    project_factory_v2,
    job_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    tmp_path,
    MockCurrentUser,
    local_resource_profile_db,
):
    """
    Test behavior for when another job with the same output_dataset_id already
    exists.
    """
    res, prof = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="dataset1"
        )
        new_task = await task_factory_v2(user_id=user.id)
        workflow1 = await workflow_factory_v2(project_id=project.id)
        workflow2 = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=new_task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=new_task.id, db=db
        )

        await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow1.id,
            working_dir=tmp_path.as_posix(),
            status="done",
            log="hello world",
        )
        job2 = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow2.id,
            working_dir=tmp_path.as_posix(),
            status="submitted",
        )

        # Test GET project/{project.id}/job/?log=false

        res1 = await client.get(f"{PREFIX}/job/")
        res2 = await client.get(f"{PREFIX}/job/?log=false")
        assert len(res1.json()) == len(res2.json()) == 2
        assert res1.json()[0]["log"] == "hello world"
        assert res2.json()[0]["log"] is None

        # Test GET project/{project.id}/workflow/{workflow.id}/job/

        res = await client.get(
            f"{PREFIX}/project/{project.id}/workflow/{workflow1.id}/job/"
        )
        assert len(res.json()) == 1

        res = await client.get(
            f"{PREFIX}/project/{project.id}/workflow/{workflow2.id}/job/"
        )
        assert len(res.json()) == 1

        # Test GET project/{project.id}/job/{job_id}/?show_tmp_logs=true

        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{job2.id}/?show_tmp_logs=true"
        )
        assert res.json()["log"] is None

        with open(f"{job2.working_dir}/{WORKFLOW_LOG_FILENAME}", "w") as f:
            f.write("hello job")

        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{job2.id}/?show_tmp_logs=true"
        )
        assert res.json()["log"] == "hello job"

        # Test GET /project/{project_id}/job/{job_id}/download/

        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{job2.id}/download/"
        )
        assert res.status_code == 200
        assert res.headers["content-type"] == "application/x-zip-compressed"
        assert "attachment;filename=" in res.headers["content-disposition"]

        # Test GET /project/{project_id}/job/?log=false

        res = await client.get(f"{PREFIX}/project/{project.id}/job/")
        assert len(res.json()) == 2
        assert res.json()[0]["log"] is not None
        assert res.json()[1]["log"] is None
        res = await client.get(f"{PREFIX}/project/{project.id}/job/?log=false")
        assert len(res.json()) == 2
        assert res.json()[0]["log"] is None
        assert res.json()[1]["log"] is None


@pytest.mark.parametrize("backend", backends_available)
async def test_stop_job(
    backend,
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    job_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    tmp_path,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=backend)

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        wf = await workflow_factory_v2(project_id=project.id)
        t = await task_factory_v2(user_id=user.id, name="task")
        ds = await dataset_factory_v2(project_id=project.id)
        await _workflow_insert_task(workflow_id=wf.id, task_id=t.id, db=db)
        job = await job_factory_v2(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=ds.id,
            workflow_id=wf.id,
        )

        debug(job)

        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{job.id}/stop/"
        )
        if backend in [ResourceType.SLURM_SUDO, ResourceType.SLURM_SSH]:
            assert res.status_code == 202

            shutdown_file = tmp_path / SHUTDOWN_FILENAME
            debug(shutdown_file)
            assert shutdown_file.exists()
        else:
            assert res.status_code == 422


async def test_update_timestamp_taskgroup(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    local_resource_profile_db,
):
    res, prof = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="dataset"
        )
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        task_group = await db.get(TaskGroupV2, task.taskgroupv2_id)
        assert task_group.timestamp_last_used > task_group.timestamp_created
        assert (
            task_group.timestamp_last_used - task_group.timestamp_created
            < timedelta(milliseconds=1)
        )
        original_timestamp_last_used = task_group.timestamp_last_used

        await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )

        await db.refresh(task_group)
        assert task_group.timestamp_last_used > original_timestamp_last_used


async def test_get_latest_jobs(
    db,
    client,
    project_factory_v2,
    job_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    tmp_path,
    MockCurrentUser,
    local_resource_profile_db,
):
    res, prof = local_resource_profile_db
    async with MockCurrentUser(
        user_kwargs=dict(
            is_verified=True,
            profile_id=prof.id,
        )
    ) as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, name="dataset"
        )
        task = await task_factory_v2(user_id=user.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )

        await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="done",
        )
        job2 = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="submitted",
        )

        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json()["id"] == job2.id

        job3 = await job_factory_v2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            working_dir=tmp_path.as_posix(),
            status="failed",
        )

        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}"
        )
        assert res.status_code == 200
        assert res.json()["id"] == job3.id

        # 404

        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id + 1}&dataset_id={dataset.id}"
        )
        assert res.status_code == 404
        res = await client.get(
            f"{PREFIX}/project/{project.id}/latest-job/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id + 1}"
        )
        assert res.status_code == 404
