"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
Tommaso Comparin <tommaso.comparin@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import os
import time
from pathlib import Path

import pytest
from devtools import debug

from .fixtures_slurm import scancel_all_jobs_of_a_slurm_user
from fractal_server.app.runner import _backends


PREFIX = "/api/v1"


backends_available = list(_backends.keys())


async def add_dummy_workflows(backend, client, project_id):
    """
    Add dummy workflows to project

    This is a workaround, to make sure that when testing the i-th backend the
    workflow has ID i. In this way, the workflow folders for each one of the
    test (with a specific backend) will have different names.
    """

    num_empty_workflows = backends_available.index(backend)
    for ind in range(num_empty_workflows):
        _ = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(
                name=f"workaround - {ind}",
                project_id=project_id,
            ),
        )


@pytest.mark.slow
@pytest.mark.parametrize("backend", backends_available)
async def test_full_workflow(
    db,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    backend,
    request,
    override_settings_factory,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
    )

    debug(f"Testing with {backend=}")
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        debug(project)
        project_id = project.id
        input_dataset = await dataset_factory(
            project, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

        # EDIT DEFAULT DATASET TO SET TYPE IMAGE

        res = await client.patch(
            f"{PREFIX}/project/{project_id}/{input_dataset_id}",
            json={"type": "image", "read_only": True},
        )
        debug(res.json())
        assert res.status_code == 200

        # ADD TEST IMAGES AS RESOURCE TO INPUT DATASET

        res = await client.post(
            f"{PREFIX}/project/{project_id}/{input_dataset_id}",
            json={
                "path": (testdata_path / "png").as_posix(),
                "glob_pattern": "*.png",
            },
        )
        debug(res.json())
        assert res.status_code == 201

        # CREATE OUTPUT DATASET AND RESOURCE

        res = await client.post(
            f"{PREFIX}/project/{project_id}/",
            json=dict(
                name="output dataset",
                type="json",
            ),
        )
        debug(res.json())
        assert res.status_code == 201
        output_dataset = res.json()
        output_dataset_id = output_dataset["id"]

        res = await client.post(
            f"{PREFIX}/project/{project_id}/{output_dataset['id']}",
            json=dict(path=tmp777_path.as_posix(), glob_pattern="out.json"),
        )
        out_resource = res.json()
        debug(out_resource)
        assert res.status_code == 201

        # CHECK WHERE WE ARE AT
        res = await client.get(f"{PREFIX}/project/{project_id}")
        debug(res.json())

        # CREATE WORKFLOW
        await add_dummy_workflows(backend, client, project_id)
        res = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(name="test workflow", project_id=project_id),
        )
        debug(res.json())
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(task_id=collect_packages[0].id),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add another (identical) dummy task, to make sure that this is allowed
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(task_id=collect_packages[0].id),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add a dummy_parallel task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(task_id=collect_packages[1].id),
        )
        debug(res.json())
        assert res.status_code == 201

        # EXECUTE WORKFLOW
        payload = dict(
            project_id=project_id,
            input_dataset_id=input_dataset_id,
            output_dataset_id=output_dataset_id,
            workflow_id=workflow_id,
            overwrite_input=False,
        )
        debug(payload)
        res = await client.post(
            f"{PREFIX}/project/apply/",
            json=payload,
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        res = await client.get(f"{PREFIX}/job/{job_data['id']}")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"

        # Verify output
        res = await client.get(
            f"{PREFIX}/project/{project_id}/{output_dataset_id}"
        )
        data = res.json()
        debug(data)
        assert "dummy" in data["meta"]

        # check that all artifacts are rw by the user running the server
        workflow_path = Path(job_status_data["working_dir"])
        no_access = []
        for f in workflow_path.glob("*"):
            has_access = os.access(f, os.R_OK | os.W_OK)
            if not has_access:
                no_access.append(f)
        debug(no_access)
        assert len(no_access) == 0


@pytest.mark.slow
@pytest.mark.parametrize("backend", backends_available)
async def test_failing_workflow(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    backend,
    request,
    override_settings_factory,
    cfut_jobs_finished,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / "artifacts",
    )

    debug(f"Testing with {backend=}")
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        project_id = project.id
        input_dataset = await dataset_factory(
            project, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

        # CREATE OUTPUT DATASET AND RESOURCE

        res = await client.post(
            f"{PREFIX}/project/{project_id}/",
            json=dict(
                name="output dataset",
                type="json",
            ),
        )
        assert res.status_code == 201
        output_dataset = res.json()
        output_dataset_id = output_dataset["id"]

        res = await client.post(
            f"{PREFIX}/project/{project_id}/{output_dataset['id']}",
            json=dict(path=tmp777_path.as_posix(), glob_pattern="out.json"),
        )
        assert res.status_code == 201

        # CREATE WORKFLOW
        await add_dummy_workflows(backend, client, project_id)
        res = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(name="test workflow", project_id=project.id),
        )
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(
                task_id=collect_packages[0].id,
                args={"raise_error": False, "sleep_time": 20}
                # task_id=collect_packages[0].id, args={"raise_error": True}
            ),
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        debug(workflow_task_id)

        # EXECUTE WORKFLOW

        payload = dict(
            project_id=project_id,
            input_dataset_id=input_dataset_id,
            output_dataset_id=output_dataset_id,
            workflow_id=workflow_id,
            overwrite_input=False,
        )
        debug("PRE apply")
        res = await client.post(
            f"{PREFIX}/project/apply/",
            json=payload,
        )
        debug("POST apply")
        assert False
        job_data = res.json()
        assert res.status_code == 202
        job_id = job_data["id"]

        res = await client.get(f"{PREFIX}/job/{job_id}")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "failed"
        assert "id: None" not in job_status_data["log"]
        assert "ValueError" in job_status_data["log"]
        assert "TASK ERROR" in job_status_data["log"]

        # For SLURM backend, a job could also fail with a JobExecutionError
        if backend == "slurm":

            # Modify the existing WorkflowTask, so that it does not raise an exception
            res = await client.patch(
                f"{PREFIX}/workflow/{workflow_id}/edit-task/{workflow_task_id}",
                json=dict(args={"raise_error": False, "sleep_time": 200}),
            )
            debug(res.json())
            assert res.status_code == 200

            # Re-submit the modified workflow
            payload = dict(
                project_id=project_id,
                input_dataset_id=input_dataset_id,
                output_dataset_id=output_dataset_id,
                workflow_id=workflow_id,
                overwrite_input=False,
            )
            res = await client.post(
                f"{PREFIX}/project/apply/",
                json=payload,
            )
            assert False

            job_data = res.json()
            assert res.status_code == 202
            job_id = job_data["id"]

            # Wait a couple of seconds, to let the SLURM job pass from PENDING
            # to RUNNING
            time.sleep(10)

            # Scancel all jobs of the current SLURM user
            scancel_all_jobs_of_a_slurm_user(
                slurm_user="test01", show_squeue=True
            )

            # Query status of the job
            res = await client.get(f"{PREFIX}/job/{job_id}")
            assert res.status_code == 200
            job_status_data = res.json()
            debug(job_status_data)
            print(job_status_data["log"])
            assert job_status_data["status"] == "failed"
            assert "id: None" not in job_status_data["log"]
            assert "JOB ERROR" in job_status_data["log"]
