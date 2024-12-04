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
import glob
import os
import shlex
import subprocess
from pathlib import Path

import pytest
from devtools import debug

from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.app.runner.v1 import _backends
from tests.fixtures_slurm import SLURM_USER

PREFIX = "/api/v1"


backends_available = list(_backends.keys())


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
    user_kwargs = {"is_verified": True}
    user_settings_dict = {}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v1")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")

        user_settings_dict["project_dir"] = user_cache_dir
        user_settings_dict["slurm_user"] = SLURM_USER

    async with MockCurrentUser(
        user_kwargs=user_kwargs,
        user_settings_dict=user_settings_dict,
    ) as user:
        debug(user)

        project = await project_factory(user)

        project_id = project.id
        input_dataset = await dataset_factory(
            project_id=project_id, name="input", type="image", read_only=True
        )
        input_dataset_id = input_dataset.id

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

        # CREATE WORKFLOW
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/",
            json=dict(name="test workflow"),
        )
        debug(res.json())
        assert res.status_code == 201
        workflow_dict = res.json()
        workflow_id = workflow_dict["id"]

        # Check project-related objects
        res = await client.get(f"{PREFIX}/project/{project_id}/workflow/")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/{project_id}/dataset/")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/project/{project_id}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 0

        # Add a dummy task
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(args=dict(number_components=11)),
        )
        debug(res.json())
        assert res.status_code == 201

        # Add another (identical) dummy task, to make sure that this is allowed
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(args=dict(number_components=11)),
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

        # Check project-related objects
        res = await client.get(f"{PREFIX}/project/{project_id}/workflow/")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/{project_id}/dataset/")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/project/{project_id}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 1

        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
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
            f"{PREFIX}/project/{project_id}/dataset/{output_dataset_id}/"
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
        for f in workflow_path.glob("**/*"):
            has_access = os.access(f, os.R_OK | os.W_OK)
            if not has_access:
                no_access.append(f)
        debug(no_access)
        assert len(no_access) == 0

        # Check that `output_dataset.meta` was updated with the `index`
        # component list
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{output_dataset_id}/"
        )
        assert res.status_code == 200
        output_dataset_json = res.json()
        debug(output_dataset_json["meta"])
        assert "index" in list(output_dataset_json["meta"].keys())


@pytest.mark.parametrize("backend", backends_available)
async def test_failing_workflow_UnknownError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    collect_packages,
    project_factory,
    dataset_factory,
    workflow_factory,
    backend,
    request,
    override_settings_factory,
    resource_factory,
):
    """
    Run a parallel task on a dataset which does not have the appropriate
    metadata (i.e. it lacks the corresponding parallelization_level component
    list), to trigger an unknown error.
    """

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path
        / f"artifacts-{backend}-UnknownError",
    )
    if backend == "slurm":
        override_settings_factory(
            FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
        )

    debug(f"Testing with {backend=}")
    user_kwargs = {"is_verified": True}
    user_settings_dict = {}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v1")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_settings_dict["project_dir"] = user_cache_dir
        user_settings_dict["slurm_user"] = SLURM_USER

    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ) as user:
        # Create project, dataset, resource
        project = await project_factory(user)
        project_id = project.id
        input_dataset = await dataset_factory(
            project_id=project_id,
            name="Input Dataset",
            type="Any",
            read_only=False,
        )
        output_dataset = await dataset_factory(
            project_id=project_id,
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
            name="test_wf", project_id=project_id
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

        res = await client.get(f"{PREFIX}/project/{project_id}/job/{job_id}/")
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "failed"
        assert job_status_data["end_timestamp"]
        assert "id: None" not in job_status_data["log"]
        assert "RuntimeError" in job_status_data["log"]
        assert "UNKNOWN ERROR" in job_status_data["log"]
        print(job_status_data["log"])


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
    user_kwargs = {"is_verified": True}
    user_settings_dict = {}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v1")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_settings_dict["project_dir"] = user_cache_dir
        user_settings_dict["slurm_user"] = SLURM_USER

    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ) as user:
        # Create project, dataset, resource
        project = await project_factory(user)
        project_id = project.id
        input_dataset = await dataset_factory(
            project_id=project_id,
            name="Input Dataset",
            type="Any",
            read_only=False,
        )
        output_dataset = await dataset_factory(
            project_id=project_id,
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
            name="test_wf", project_id=project_id
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

        res = await client.get(f"{PREFIX}/project/{project_id}/job/{job_id}/")
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
                f"{PREFIX}/project/{project_id}/dataset/{output_dataset.id}/"
            )
            assert res.status_code == 200
            output_dataset_json = res.json()
            debug(output_dataset_json["meta"])
            assert "index" in list(output_dataset_json["meta"].keys())


async def test_failing_workflow_JobExecutionError_slurm(
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
    relink_python_interpreter_v1,
    resource_factory,
    tmp_path,
):

    override_settings_factory(
        FRACTAL_RUNNER_BACKEND="slurm",
        FRACTAL_RUNNER_WORKING_BASE_DIR=(tmp777_path / "artifacts"),
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json",
    )

    user_cache_dir = str(tmp777_path / "user_cache_dir")
    user_kwargs = dict(is_verified=True)
    user_settings_dict = dict(
        project_dir=user_cache_dir, slurm_user=SLURM_USER
    )
    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ) as user:
        project = await project_factory(user)
        project_id = project.id
        input_dataset = await dataset_factory(
            project_id=project_id,
            name="input_dataset",
            type="Any",
            read_only=False,
        )
        output_dataset = await dataset_factory(
            project_id=project_id,
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
            name="test_wf", project_id=project_id
        )

        # Add a short task, which will be run successfully
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(args={"raise_error": False, "sleep_time": 0.1}),
        )
        assert res.status_code == 201
        wftask0_id = res.json()["id"]
        debug(wftask0_id)

        # Add a long task, which will be stopped while running
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={collect_packages[0].id}",
            json=dict(args={"raise_error": False, "sleep_time": 200}),
        )
        assert res.status_code == 201
        wftask1_id = res.json()["id"]
        debug(wftask1_id)

        # NOTE: the client.post call below is blocking, due to the way we are
        # running tests. For this reason, we call the scancel function from a
        # from a subprocess.Popen, so that we can make it happen during the
        # execution.
        scancel_sleep_time = 10
        slurm_user = SLURM_USER

        tmp_script = (tmp_path / "script.sh").as_posix()
        debug(tmp_script)
        with open(tmp_script, "w") as f:
            f.write(f"sleep {scancel_sleep_time}\n")
            f.write(
                (
                    f"sudo --non-interactive -u {slurm_user} "
                    f"scancel -u {slurm_user} -v"
                    "\n"
                )
            )

        tmp_stdout = open((tmp_path / "stdout").as_posix(), "w")
        tmp_stderr = open((tmp_path / "stderr").as_posix(), "w")
        subprocess.Popen(
            shlex.split(f"bash {tmp_script}"),
            stdout=tmp_stdout,
            stderr=tmp_stderr,
        )

        # Re-submit the modified workflow
        res_second_apply = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
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
        res = await client.get(f"{PREFIX}/project/{project_id}/job/{job_id}/")
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
            f"api/v1/project/{project_id}/dataset/{output_dataset.id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert statuses == {
            str(wftask0_id): "done",
            str(wftask1_id): "failed",
        }

        tmp_stdout.close()
        tmp_stderr.close()


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
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
        # Create project
        project = await project_factory(user)
        project_id = project.id

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project_id
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
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        assert res.status_code == 201

        # Create datasets
        dataset = await dataset_factory(
            project_id=project_id, name="dataset", type="zarr", read_only=False
        )
        await resource_factory(path=str(tmp_path / "data"), dataset=dataset)

        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}"
            f"&output_dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check that the workflow execution is complete
        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"
        debug(job_status_data["end_timestamp"])
        assert job_status_data["end_timestamp"]

        # Check that the expected files are present
        working_dir = job_status_data["working_dir"]
        glob_list = [Path(x).name for x in glob.glob(f"{working_dir}/*")] + [
            Path(x).name for x in glob.glob(f"{working_dir}/**/*")
        ]
        must_exist = [
            "0.args.json",
            "0.err",
            "0.metadiff.json",
            "0.out",
            WORKFLOW_LOG_FILENAME,
        ]
        for f in must_exist:
            assert f in glob_list

        # Check that stderr and stdout are as expected
        with open(f"{working_dir}/0_non_python/0.out", "r") as f:
            out = f.read()
        assert "This goes to standard output" in out
        with open(f"{working_dir}/0_non_python/0.err", "r") as f:
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
    async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
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
        project_id = project.id
        workflow = await workflow_factory(
            name="test_wf", project_id=project_id
        )
        for task in (task0, task1, task2, task3):
            res = await client.post(
                f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
                f"?task_id={task.id}",
                json=dict(),
            )
            assert res.status_code == 201

        dataset = await dataset_factory(
            project_id=project_id,
            name="dataset",
            type="zarr",
            read_only=False,
            meta=dict(index=["A", "B"]),
        )
        await resource_factory(path=str(tmp_path / "data"), dataset=dataset)
        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}"
            f"&output_dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check that the workflow execution is complete
        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["status"] == "done"
        debug(job_status_data["end_timestamp"])
        assert job_status_data["end_timestamp"]

        # Check that the expected files are present
        working_dir = job_status_data["working_dir"]
        glob_list = [Path(x).name for x in glob.glob(f"{working_dir}/*")] + [
            Path(x).name for x in glob.glob(f"{working_dir}/**/*")
        ]
        debug(glob_list)
        must_exist = [
            "0.args.json",
            "0.err",
            "0.out",
            "1_par_a.args.json",
            "1_par_a.err",
            "1_par_a.out",
            "1_par_b.args.json",
            "1_par_b.err",
            "1_par_b.out",
            "2.args.json",
            "2.err",
            "2.out",
            "2.metadiff.json",
            "3_par_a.args.json",
            "3_par_a.err",
            "3_par_a.out",
            "3_par_b.args.json",
            "3_par_b.err",
            "3_par_b.out",
            "3_par_a.metadiff.json",
            "3_par_b.metadiff.json",
            WORKFLOW_LOG_FILENAME,
        ]
        for f in must_exist:
            assert f in glob_list

        # Check that workflow.log includes expected warnings
        with open(f"{working_dir}/{WORKFLOW_LOG_FILENAME}", "r") as f:
            logs = f.read()
        print(logs)
        assert "Skip collection of updated metadata" in logs


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
    user_kwargs = {"is_verified": True}
    user_settings_dict = {}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v1")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_settings_dict["project_dir"] = user_cache_dir
        user_settings_dict["slurm_user"] = SLURM_USER

    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ) as user:
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
        project_id = project.id

        # Create workflow
        workflow = await workflow_factory(
            name="test_wf", project_id=project_id
        )

        # Add task to workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        debug(res.json())
        assert res.status_code == 201

        # Create dataset
        dataset = await dataset_factory(
            project_id=project_id, name="input", type="zarr", read_only=False
        )
        await resource_factory(path=str(tmp_path / "dir"), dataset=dataset)

        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={dataset.id}"
            f"&output_dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check that the workflow execution failed as expected
        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
        )
        assert res.status_code == 200
        job = res.json()
        debug(job)
        assert job["status"] == "failed"
        assert "Hint: make sure that it is executable" in job["log"]
