"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Marco Franzon <marco.franzon@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import asyncio
import logging
import os
import threading
import time
from pathlib import Path

import pytest
from devtools import debug

from .fixtures_slurm import scancel_all_jobs_of_a_slurm_user
from fractal_server.app.runner import _backends


PREFIX = "/api/v1"


backends_available = list(_backends.keys())


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
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
    )

    debug(f"Testing with {backend=}")
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")
        request.getfixturevalue("cfut_jobs_finished")

    async with MockCurrentUser(persist=True) as user:
        project_dir = tmp777_path / f"project_dir-{backend}"
        project = await project_factory(user, project_dir=str(project_dir))

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
            json=dict(path=tmp777_path.as_posix()),
        )
        out_resource = res.json()
        debug(out_resource)
        assert res.status_code == 201

        # CHECK WHERE WE ARE AT
        res = await client.get(f"{PREFIX}/project/{project_id}")
        debug(res.json())

        # CREATE WORKFLOW
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

        # Check that all files in working_dir are RW for the user running the
        # server. Note that the same is **not** true for files in
        # working_dir_user.
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
async def test_failing_workflow_TaskExecutionError(
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
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / f"artifacts-{backend}-TaskExecutionError",
    )

    debug(f"Testing with {backend=}")
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")
        request.getfixturevalue("cfut_jobs_finished")

    async with MockCurrentUser(persist=True) as user:
        project_dir = tmp777_path / f"project_dir-{backend}-TaskExecutionError"
        project = await project_factory(user, project_dir=str(project_dir))
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
            json=dict(path=tmp777_path.as_posix()),
        )
        assert res.status_code == 201

        # CREATE WORKFLOW
        res = await client.post(
            f"{PREFIX}/workflow/",
            json=dict(name="test workflow", project_id=project.id),
        )
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        ERROR_MESSAGE = "this is a nice error"
        res = await client.post(
            f"{PREFIX}/workflow/{workflow_id}/add-task/",
            json=dict(
                task_id=collect_packages[0].id,
                args={"raise_error": True, "message": ERROR_MESSAGE},
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
        res = await client.post(
            f"{PREFIX}/project/apply/",
            json=payload,
        )
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
        assert ERROR_MESSAGE in job_status_data["log"]
        assert "TASK ERROR" in job_status_data["log"]
        assert "\\n" not in job_status_data["log"]
        print(job_status_data["log"])
        # Check that ERROR_MESSAGE only appears once in the logs:
        assert len(job_status_data["log"].split(ERROR_MESSAGE)) == 2


async def _auxiliary_scancel(slurm_user, sleep_time):
    # The _auxiliary_scancel and _auxiliary_run functions are used as in
    # https://stackoverflow.com/a/59645689/19085332
    logging.warning(f"[scancel_thread] run START {time.perf_counter()=}")
    # Wait `scancel_sleep_time` seconds, to let the SLURM job pass from PENDING
    # to RUNNING
    time.sleep(sleep_time)
    # Scancel all jobs of the current SLURM user
    logging.warning(f"[scancel_thread] run SCANCEL {time.perf_counter()=}")
    scancel_all_jobs_of_a_slurm_user(slurm_user=slurm_user, show_squeue=True)
    logging.warning(f"[scancel_thread] run END {time.perf_counter()=}")


def _auxiliary_run(slurm_user, sleep_time):
    # The _auxiliary_scancel and _auxiliary_run functions are used as in
    # https://stackoverflow.com/a/59645689/19085332
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_auxiliary_scancel(slurm_user, sleep_time))
    loop.close()


@pytest.mark.slow
async def test_failing_workflow_JobExecutionError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    request,
    override_settings_factory,
    monkey_slurm,
    monkey_slurm_user,
    relink_python_interpreter,
    cfut_jobs_finished,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm",
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / "artifacts-test_failing_workflow_JobExecutionError",
    )

    async with MockCurrentUser(persist=True) as user:
        project_dir = tmp777_path / "project_dir-JobExecutionError"
        project = await project_factory(user, project_dir=str(project_dir))
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
            json=dict(path=tmp777_path.as_posix()),
        )
        assert res.status_code == 201

        # CREATE WORKFLOW
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
                args={"raise_error": False, "sleep_time": 200},
            ),
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        debug(workflow_task_id)

        # NOTE: the client.post call below is blocking, due to the way we are
        # running tests. For this reason, we call the scancel functionfrom a
        # different thread, so that we can make it happen during the workflow
        # execution
        # The following block is based on
        # https://stackoverflow.com/a/59645689/19085332
        scancel_sleep_time = 10
        slurm_user = monkey_slurm_user
        logging.warning(f"PRE THREAD START {time.perf_counter()=}")
        _thread = threading.Thread(
            target=_auxiliary_run, args=(slurm_user, scancel_sleep_time)
        )
        _thread.start()
        logging.warning(f"POST THREAD START {time.perf_counter()=}")

        # Re-submit the modified workflow
        payload = dict(
            project_id=project_id,
            input_dataset_id=input_dataset_id,
            output_dataset_id=output_dataset_id,
            workflow_id=workflow_id,
            overwrite_input=False,
        )
        res_second_apply = await client.post(
            f"{PREFIX}/project/apply/",
            json=payload,
        )
        job_data = res_second_apply.json()
        debug(job_data)
        assert res_second_apply.status_code == 202
        job_id = job_data["id"]
        debug(job_id)

        # Query status of the job
        res = await client.get(f"{PREFIX}/job/{job_id}")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        print(job_status_data["log"])
        assert job_status_data["status"] == "failed"
        assert "id: None" not in job_status_data["log"]
        assert "JOB ERROR" in job_status_data["log"]
        assert "CANCELLED" in job_status_data["log"]
        assert "\\n" not in job_status_data["log"]
