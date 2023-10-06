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

import pytest
from devtools import debug

from .fixtures_slurm import scancel_all_jobs_of_a_slurm_user
from fractal_server.app.runner import _backends

PREFIX = "/api/v1"


backends_available = list(_backends.keys())


@pytest.mark.parametrize(
    "override_settings_startup, backend",
    [
        ({"FRACTAL_RUNNER_BACKEND": backend}, backend)
        for backend in backends_available
    ],
    indirect=["override_settings_startup"],
)
@pytest.mark.slow
async def test_full_workflow(
    db,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    request,
    backend,
    override_settings_startup,
    override_settings_runtime,
):
    override_settings_runtime(
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
    )
    if backend == "slurm":
        override_settings_runtime(
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
            project_id=project.id, name="input", type="image", read_only=True
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

        # Check that `output_dataset.meta` was updated with the `index`
        # component list
        res = await client.get(
            f"{PREFIX}/project/{project.id}/dataset/{output_dataset_id}"
        )
        assert res.status_code == 200
        output_dataset_json = res.json()
        debug(output_dataset_json["meta"])
        assert "index" in list(output_dataset_json["meta"].keys())


@pytest.mark.slow
@pytest.mark.parametrize(
    "override_settings_startup, backend",
    [
        ({"FRACTAL_RUNNER_BACKEND": backend}, backend)
        for backend in backends_available
    ],
    indirect=["override_settings_startup"],
)
async def test_failing_workflow_UnknownError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    workflow_factory,
    request,
    resource_factory,
    override_settings_startup,
    backend,
    override_settings_runtime,
):
    """
    Run a parallel task on a dataset which does not have the appropriate
    metadata (i.e. it lacks the corresponding parallelization_level component
    list), to trigger an unknown error.
    """

    override_settings_runtime(
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / f"artifacts-{backend}-UnknownError",
    )
    if backend == "slurm":
        override_settings_runtime(
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
        input_dataset = await dataset_factory(
            project_id=project.id,
            name="Input Dataset",
            type="Any",
            read_only=False,
        )
        output_dataset = await dataset_factory(
            project_id=project.id,
            name="Output Dataset",
            type="Any",
            read_only=False,
        )
        await resource_factory(
            path=str(tmp777_path / "data_in"), dataset=input_dataset
        )
        await resource_factory(
            path=str(tmp777_path / "data_out"), dataset=output_dataset
        )

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project.id
        )

        # Add a (parallel) dummy_parallel task
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[1].id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 201

        # Execute workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
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
        assert "RuntimeError" in job_status_data["log"]
        assert "UNKNOWN ERROR" in job_status_data["log"]
        print(job_status_data["log"])


@pytest.mark.slow
@pytest.mark.parametrize(
    "override_settings_startup, backend",
    [
        ({"FRACTAL_RUNNER_BACKEND": backend}, backend)
        for backend in backends_available
    ],
    indirect=["override_settings_startup"],
)
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
    failing_task: str,
    request,
    resource_factory,
    backend,
    override_settings_startup,
    override_settings_runtime,
):

    override_settings_runtime(
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / f"artifacts-{backend}-TaskExecutionError-{failing_task}",
    )
    if backend == "slurm":
        override_settings_runtime(
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
        input_dataset = await dataset_factory(
            project_id=project.id,
            name="Input Dataset",
            type="Any",
            read_only=False,
        )
        output_dataset = await dataset_factory(
            project_id=project.id,
            name="Output Dataset",
            type="Any",
            read_only=False,
        )
        await resource_factory(
            path=str(tmp777_path / "data_in"), dataset=input_dataset
        )
        await resource_factory(
            path=str(tmp777_path / "data_out"), dataset=output_dataset
        )

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
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
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
            f"api/v1/project/{project_id}/dataset/{output_dataset.id}/status/"
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
            f"api/v1/project/{project_id}/"
            f"dataset/{output_dataset.id}/export_history/"
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

        # If the first task went through, then check that output_dataset.meta
        # was updated (ref issue #844)
        if failing_task == "parallel":
            res = await client.get(
                f"{PREFIX}/project/{project.id}/dataset/{output_dataset.id}"
            )
            assert res.status_code == 200
            output_dataset_json = res.json()
            debug(output_dataset_json["meta"])
            assert "index" in list(output_dataset_json["meta"].keys())


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


@pytest.mark.parametrize(
    "override_settings_startup, backend",
    [({"FRACTAL_RUNNER_BACKEND": "slurm"}, "slurm")],
    indirect=["override_settings_startup"],
)
@pytest.mark.slow
async def test_failing_workflow_JobExecutionError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    workflow_factory,
    request,
    monkey_slurm,
    monkey_slurm_user,
    relink_python_interpreter,
    cfut_jobs_finished,
    resource_factory,
    backend,
    override_settings_startup,
    override_settings_runtime,
):
    override_settings_runtime(
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / f"artifacts-{backend}-test_failing_workflow_JobExecutionError",
    )
    if backend == "slurm":
        override_settings_runtime(
            FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
        )

    user_cache_dir = str(tmp777_path / "user_cache_dir")
    user_kwargs = dict(cache_dir=user_cache_dir)
    async with MockCurrentUser(persist=True, user_kwargs=user_kwargs) as user:
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project_id=project.id,
            name="input_dataset",
            type="Any",
            read_only=False,
        )
        output_dataset = await dataset_factory(
            project_id=project.id,
            name="output_dataset",
            type="Any",
            read_only=False,
        )
        await resource_factory(
            path=str(tmp777_path / "input_dir"), dataset=input_dataset
        )
        await resource_factory(
            path=str(tmp777_path / "output_dir"), dataset=output_dataset
        )

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project.id
        )

        # Add a short task, which will be run successfully
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(args={"raise_error": False, "sleep_time": 0.1}),
        )
        assert res.status_code == 201
        wftask0_id = res.json()["id"]
        debug(wftask0_id)

        # Add a long task, which will be stopped while running
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(args={"raise_error": False, "sleep_time": 200}),
        )
        assert res.status_code == 201
        wftask1_id = res.json()["id"]
        debug(wftask1_id)

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
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
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
            f"api/v1/project/{project.id}/dataset/{output_dataset.id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert statuses == {
            str(wftask0_id): "done",
            str(wftask1_id): "failed",
        }


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
            project_id=project.id, name="dataset", type="zarr", read_only=False
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


async def test_metadiff(
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
    Run task with command which does not produce metadiff files, or which
    produces a single `null` value rather than a dictionary. See issues 854 and
    878.
    """
    task_file = str(testdata_path / "echo_sleep_task.sh")
    task_file2 = str(testdata_path / "non_python_task_issue878.sh")
    command = f"bash {task_file}"
    command_null = f"bash {task_file2}"
    async with MockCurrentUser(persist=True) as user:
        task0 = await task_factory(
            name="task0",
            source="task0",
            command=command,
            input_type="Any",
            output_type="Any",
        )
        task1 = await task_factory(
            name="task1",
            source="task1",
            command=command,
            input_type="Any",
            output_type="Any",
            meta=dict(parallelization_level="index"),
        )
        task2 = await task_factory(
            name="task2",
            source="task2",
            command=command_null,
            input_type="Any",
            output_type="Any",
        )
        task3 = await task_factory(
            name="task3",
            source="task3",
            command=command_null,
            input_type="Any",
            output_type="Any",
            meta=dict(parallelization_level="index"),
        )

        project = await project_factory(user)
        workflow = await workflow_factory(
            name="test_wf", project_id=project.id
        )
        for task in (task0, task1, task2, task3):
            res = await client.post(
                f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/wftask/"
                f"?task_id={task.id}",
                json=dict(),
            )
            assert res.status_code == 201

        dataset = await dataset_factory(
            project_id=project.id,
            name="dataset",
            type="zarr",
            read_only=False,
            meta=dict(index=["A", "B"]),
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
        debug(glob_list)
        must_exist = [
            "0.args.json",
            "0.err",
            "0.out",
            "1_par_A.args.json",
            "1_par_A.err",
            "1_par_A.out",
            "1_par_B.args.json",
            "1_par_B.err",
            "1_par_B.out",
            "2.args.json",
            "2.err",
            "2.out",
            "2.metadiff.json",
            "3_par_A.args.json",
            "3_par_A.err",
            "3_par_A.out",
            "3_par_B.args.json",
            "3_par_B.err",
            "3_par_B.out",
            "3_par_A.metadiff.json",
            "3_par_B.metadiff.json",
            "workflow.log",
        ]
        for f in must_exist:
            assert f in glob_list

        # Check that workflow.log includes expected warnings
        with open(f"{working_dir}/workflow.log", "r") as f:
            logs = f.read()
        assert "Skip collection of updated metadata" in logs
        assert (
            "Skip collection and aggregation of parallel-task updated metadata."  # noqa
            in logs
        )


@pytest.mark.parametrize(
    "override_settings_startup, backend",
    [
        ({"FRACTAL_RUNNER_BACKEND": backend}, backend)
        for backend in backends_available
    ],
    indirect=["override_settings_startup"],
)
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
    request,
    tmp_path,
    backend,
    override_settings_startup,
    override_settings_runtime,
):
    """
    Execute a workflow with a task which has an invalid `command` (i.e. it is
    not executable).
    """
    override_settings_runtime(
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
    )
    if backend == "slurm":
        override_settings_runtime(
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
            project_id=project.id, name="input", type="zarr", read_only=False
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
