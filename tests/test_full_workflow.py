"""
Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
University of Zurich

Original author(s):
Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
Tommaso Comparin <tommaso.comparin@exact-lab.it>
Marco Franzon <marco.franzon@exact-lab.it>
Yuri Chiucconi <yuri.chiucconi@exact-lab.it>

This file is part of Fractal and was originally developed by eXact lab S.r.l.
<exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
Institute for Biomedical Research and Pelkmans Lab from the University of
Zurich.
"""
import asyncio
import glob
import logging
import os
import threading
import time
from pathlib import Path

import pytest  # type: ignore[import]
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
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
    )
    if backend == "slurm":
        override_settings_factory(
            FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
        )

    debug(f"Testing with {backend=}")

    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")
        request.getfixturevalue("cfut_jobs_finished")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_kwargs = dict(cache_dir=user_cache_dir)
    else:
        user_kwargs = {}

    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs) as user:
        debug(user)

        project = await project_factory(user)

        debug(project)
        project_id = project.id
        input_dataset = await dataset_factory(
            project, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

        # EDIT DEFAULT DATASET TO SET TYPE IMAGE
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/dataset/{input_dataset_id}",
            json={"type": "image", "read_only": True},
        )
        debug(res.json())
        assert res.status_code == 200

        # ADD TEST IMAGES AS RESOURCE TO INPUT DATASET
        res = await client.post(
            f"{PREFIX}/project/{project_id}/"
            f"dataset/{input_dataset_id}/resource/",
            json={
                "path": (testdata_path / "png").as_posix(),
            },
        )
        debug(res)
        debug(res.json())
        assert res.status_code == 201

        # CREATE OUTPUT DATASET AND RESOURCE
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/",
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
            f"{PREFIX}/project/{project_id}/"
            f"dataset/{output_dataset['id']}/resource/",
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
            f"{PREFIX}/project/{project_id}/workflow/",
            json=dict(name="test workflow"),
        )
        debug(res.json())
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add another (identical) dummy task, to make sure that this is allowed
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add a dummy_parallel task
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={collect_packages[1].id}",
            json=dict(),
        )
        debug(res.json())
        assert res.status_code == 201

        # EXECUTE WORKFLOW
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/apply/"
            f"?input_dataset_id={input_dataset_id}"
            f"&output_dataset_id={output_dataset_id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"
        assert job_status_data["log"]
        assert "START workflow" in job_status_data["log"]
        assert "END workflow" in job_status_data["log"]

        # Verify output
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{output_dataset_id}"
        )
        data = res.json()
        debug(data)
        assert "dummy" in data["meta"]

        # Test get_workflowtask_status endpoint
        res = await client.get(
            f"api/v1/project/{project_id}/dataset/{output_dataset_id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert set(statuses.values()) == {"done"}

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
@pytest.mark.parametrize("failing_task", ["parallel", "non_parallel"])
async def test_failing_workflow_TaskExecutionError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    workflow_factory,
    backend,
    failing_task: str,
    request,
    override_settings_factory,
    resource_factory,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / f"artifacts-{backend}-TaskExecutionError-{failing_task}",
    )
    if backend == "slurm":
        override_settings_factory(
            FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
        )

    debug(f"Testing with {backend=}")
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")
        request.getfixturevalue("cfut_jobs_finished")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_kwargs = dict(cache_dir=user_cache_dir)
    else:
        user_kwargs = {}
    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs) as user:
        # Create project, dataset, resource
        project = await project_factory(user)
        project_id = project.id
        dataset = await dataset_factory(
            project, name="My Dataset", type="Any", read_only=False
        )
        await resource_factory(path=str(tmp777_path / "data"), dataset=dataset)

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project.id
        )

        # Prepare payloads for adding non-parallel and parallel dummy tasks
        payload_non_parallel = dict()
        payload_parallel = dict()
        ERROR_MESSAGE = f"this is a nice error for a {failing_task} task"
        failing_args = {"raise_error": True, "message": ERROR_MESSAGE}
        if failing_task == "non_parallel":
            payload_non_parallel["args"] = failing_args
        elif failing_task == "parallel":
            payload_parallel["args"] = failing_args

        # Add a (non-parallel) dummy task
        debug(payload_non_parallel)
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=payload_non_parallel,
        )
        debug(res.json())
        assert res.status_code == 201
        ID_NON_PARALLEL_WFTASK = res.json()["id"]

        # Add a (parallel) dummy_parallel task
        debug(payload_parallel)
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[1].id}",
            json=payload_parallel,
        )
        debug(res.json())
        assert res.status_code == 201
        ID_PARALLEL_WFTASK = res.json()["id"]

        # Execute workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}"
            f"&output_dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        assert res.status_code == 202
        job_id = job_data["id"]

        res = await client.get(f"{PREFIX}/project/{project_id}/job/{job_id}")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "failed"
        assert job_status_data["end_timestamp"]
        assert "id: None" not in job_status_data["log"]
        assert "ValueError" in job_status_data["log"]
        assert ERROR_MESSAGE in job_status_data["log"]
        assert "TASK ERROR" in job_status_data["log"]
        assert "\\n" not in job_status_data["log"]
        print(job_status_data["log"])

        # Check that ERROR_MESSAGE only appears once in the logs:
        assert len(job_status_data["log"].split(ERROR_MESSAGE)) == 2

        # Test get_workflowtask_status endpoint
        res = await client.get(
            f"api/v1/project/{project_id}/dataset/{dataset.id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        if failing_task == "non_parallel":
            assert statuses == {str(ID_NON_PARALLEL_WFTASK): "failed"}
        else:
            assert statuses == {
                str(ID_NON_PARALLEL_WFTASK): "done",
                str(ID_PARALLEL_WFTASK): "failed",
            }

        # Test export_history_as_workflow endpoint, and that
        res = await client.get(
            f"api/v1/project/{project_id}/dataset/{dataset.id}/export_history/"
        )
        assert res.status_code == 200
        exported_wf = res.json()
        debug(exported_wf)
        res = await client.post(
            f"api/v1/project/{project_id}/workflow/import/",
            json=exported_wf,
        )
        assert res.status_code == 201
        debug(res.json())


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


@pytest.mark.parametrize("backend", ["slurm"])
@pytest.mark.slow
async def test_failing_workflow_JobExecutionError(
    backend,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    workflow_factory,
    request,
    override_settings_factory,
    monkey_slurm,
    monkey_slurm_user,
    relink_python_interpreter,
    cfut_jobs_finished,
    resource_factory,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / f"artifacts-{backend}-test_failing_workflow_JobExecutionError",
    )
    if backend == "slurm":
        override_settings_factory(
            FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
        )

    user_cache_dir = str(tmp777_path / "user_cache_dir")
    user_kwargs = dict(cache_dir=user_cache_dir)
    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(
            project, name="dataset", type="Any", read_only=False
        )
        await resource_factory(
            path=str(tmp777_path / "input_dir"), dataset=dataset
        )

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project.id
        )

        # FIXME: add two tasks, with the first one with raise_error=False and
        # sleep_time=1

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(args={"raise_error": False, "sleep_time": 200}),
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
        res_second_apply = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}"
            f"&output_dataset_id={dataset.id}",
            json={},
        )
        job_data = res_second_apply.json()
        debug(job_data)
        assert res_second_apply.status_code == 202
        job_id = job_data["id"]
        debug(job_id)

        # Query status of the job
        res = await client.get(f"{PREFIX}/project/{project.id}/job/{job_id}")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        print(job_status_data["log"])
        assert job_status_data["status"] == "failed"
        assert job_status_data["end_timestamp"]
        assert "id: None" not in job_status_data["log"]
        assert "JOB ERROR" in job_status_data["log"]
        assert "CANCELLED" in job_status_data["log"]
        assert "\\n" not in job_status_data["log"]

        # Test get_workflowtask_status endpoint
        res = await client.get(
            f"api/v1/project/{project.id}/dataset/{dataset.id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert statuses[str(workflow_task_id)] == "failed"


async def test_non_python_task(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    resource_factory,
    task_factory,
    testdata_path,
    tmp_path,
):
    """
    Run a full workflow with a single bash task, which simply writes something
    to stderr and stdout
    """
    async with MockCurrentUser(persist=True) as user:
        # Create project
        project = await project_factory(user)

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project.id
        )

        # Create task
        task = await task_factory(
            name="non-python",
            source="custom-task",
            command=f"bash {str(testdata_path)}/non_python_task_issue189.sh",
            input_type="zarr",
            output_type="zarr",
        )

        # Add task to workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        assert res.status_code == 201

        # Create datasets
        dataset = await dataset_factory(
            project, name="dataset", type="zarr", read_only=False
        )
        await resource_factory(path=str(tmp_path / "data"), dataset=dataset)

        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}"
            f"&output_dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check that the workflow execution is complete
        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{job_data['id']}"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"
        debug(job_status_data["end_timestamp"])
        assert job_status_data["end_timestamp"]

        # Check that the expected files are present
        working_dir = job_status_data["working_dir"]
        glob_list = [Path(x).name for x in glob.glob(f"{working_dir}/*")]
        must_exist = [
            "0.args.json",
            "0.err",
            "0.metadiff.json",
            "0.out",
            "workflow.log",
        ]
        for f in must_exist:
            assert f in glob_list

        # Check that stderr and stdout are as expected
        with open(f"{working_dir}/0.out", "r") as f:
            out = f.read()
        assert "This goes to standard output" in out
        with open(f"{working_dir}/0.err", "r") as f:
            err = f.read()
        assert "This goes to standard error" in err


@pytest.mark.parametrize("backend", backends_available)
async def test_non_executable_task_command(
    db,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    task_factory,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    backend,
    request,
    override_settings_factory,
    tmp_path,
):
    """
    Execute a workflow with a task which has an invalid `command` (i.e. it is
    not executable).
    """
    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
    )
    if backend == "slurm":
        override_settings_factory(
            FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
        )

    debug(f"Testing with {backend=}")

    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter")
        request.getfixturevalue("cfut_jobs_finished")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_kwargs = dict(cache_dir=user_cache_dir)
    else:
        user_kwargs = {}

    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs) as user:

        # Create task
        task = await task_factory(
            name="invalid-task-command",
            source="some_source",
            command=str(testdata_path / "non_executable_task.sh"),
            input_type="zarr",
            output_type="zarr",
        )
        debug(task)

        # Create project
        project = await project_factory(user)

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project.id
        )

        # Add task to workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        debug(res.json())
        assert res.status_code == 201

        # Create dataset
        dataset = await dataset_factory(
            project, name="input", type="zarr", read_only=False
        )
        await resource_factory(path=str(tmp_path / "dir"), dataset=dataset)

        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}"
            f"&output_dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check that the workflow execution failed as expected
        res = await client.get(
            f"{PREFIX}/project/{project.id}/job/{job_data['id']}"
        )
        assert res.status_code == 200
        job = res.json()
        debug(job)
        assert job["status"] == "failed"
        assert "Hint: make sure that it is executable" in job["log"]
