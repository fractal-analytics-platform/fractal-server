import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

import pytest
from devtools import debug

from fractal_server.app.runner.v2 import _backends


def _task_name_to_id(task_name: str, task_list: list[dict[str, Any]]) -> int:
    task_id = next(
        task["id"] for task in task_list if task["name"] == task_name
    )
    return task_id


PREFIX = "/api/v2"

backends_available = list(_backends.keys())


@pytest.mark.parametrize("backend", backends_available)
async def test_full_workflow(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    backend,
    override_settings_factory,
    tmp_path_factory,
    fractal_tasks_mock,
    request,
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    basetemp = tmp_path_factory.getbasetemp()
    FRACTAL_TASKS_DIR = basetemp / "FRACTAL_TASKS_DIR"
    selected_new_settings = dict(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
        FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR,
    )
    if backend == "slurm":
        selected_new_settings.update(
            dict(FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json")
        )
    override_settings_factory(**selected_new_settings)

    debug(f"Testing with {backend=}")
    user_kwargs = {"is_verified": True}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v2")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_kwargs["cache_dir"] = user_cache_dir

    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        project = await project_factory_v2(user)
        project_id = project.id
        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="dataset",
        )
        dataset_id = dataset.id
        workflow = await workflow_factory_v2(
            project_id=project_id, name="workflow"
        )
        workflow_id = workflow.id

        # Check project-related objects
        res = await client.get(f"{PREFIX}/project/{project_id}/workflow/")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/{project_id}/dataset/")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/{project_id}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 0

        # Retrieve task list
        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 200
        task_list = res.json()

        # Add "create_ome_zarr_compound" task
        task_id_A = _task_name_to_id(
            task_name="create_ome_zarr_compound", task_list=task_list
        )
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id_A}",
            json=dict(args_non_parallel=dict(image_dir="/somewhere")),
        )
        assert res.status_code == 201
        # Add "MIP_compound" task
        task_id_B = _task_name_to_id(
            task_name="MIP_compound", task_list=task_list
        )
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id_B}",
            json={},
        )
        assert res.status_code == 201

        # EXECUTE WORKFLOW
        res = await client.post(
            f"{PREFIX}/project/{project_id}/job/submit/"
            f"?workflow_id={workflow_id}&dataset_id={dataset_id}",
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
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/{project_id}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 1

        # Check job
        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        assert job_status_data["log"]
        debug(job_status_data["working_dir"])
        assert job_status_data["status"] == "done"
        assert "START workflow" in job_status_data["log"]
        assert "END workflow" in job_status_data["log"]

        # Check that all files in working_dir are RW for the user running the
        # server. Note that the same is **not** true for files in
        # working_dir_user.
        workflow_path = Path(job_status_data["working_dir"])
        non_accessible_files = []
        for f in workflow_path.glob("*"):
            has_access = os.access(f, os.R_OK | os.W_OK)
            if not has_access:
                non_accessible_files.append(f)
        debug(non_accessible_files)
        assert len(non_accessible_files) == 0

        # Check output dataset and image
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/"
        )
        assert res.status_code == 200
        dataset = res.json()
        debug(dataset)
        assert len(dataset["history"]) == 2
        assert dataset["filters"]["types"] == {"3D": False}
        # assert dataset["filters"]["attributes"] == {}
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/"
            "images/query/",
            json={},
        )
        assert res.status_code == 200
        image_page = res.json()
        debug(image_page)
        # There should be two 3D images and two 2D images
        assert image_page["total_count"] == 4
        images = image_page["images"]
        debug(images)
        images_3D = filter(lambda img: img["types"]["3D"], images)
        images_2D = filter(lambda img: not img["types"]["3D"], images)
        assert len(list(images_2D)) == 2
        assert len(list(images_3D)) == 2

        # Test get_workflowtask_status endpoint
        res = await client.get(
            (
                f"{PREFIX}/project/{project_id}/status/?"
                f"dataset_id={dataset_id}&workflow_id={workflow_id}"
            )
        )

        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert set(statuses.values()) == {"done"}


@pytest.mark.parametrize("backend", backends_available)
async def test_full_workflow_TaskExecutionError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    backend,
    request,
    override_settings_factory,
    fractal_tasks_mock,
    tmp_path_factory,
):
    """ "
    Run a workflow made of three tasks, two successful tasks and one
    that raises an error.
    """
    EXPECTED_STATUSES = {}

    # Use a session-scoped FRACTAL_TASKS_DIR folder
    basetemp = tmp_path_factory.getbasetemp()
    FRACTAL_TASKS_DIR = basetemp / "FRACTAL_TASKS_DIR"
    selected_new_settings = dict(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
        FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR,
    )
    if backend == "slurm":
        selected_new_settings.update(
            dict(FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json")
        )
    override_settings_factory(**selected_new_settings)

    debug(f"Testing with {backend=}")
    user_kwargs = {"is_verified": True}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v2")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_kwargs["cache_dir"] = user_cache_dir

    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        project = await project_factory_v2(user)
        project_id = project.id
        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="dataset",
        )
        dataset_id = dataset.id
        workflow = await workflow_factory_v2(
            project_id=project_id, name="workflow"
        )
        workflow_id = workflow.id

        # Retrieve task list
        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 200
        task_list = res.json()

        # Add "create_ome_zarr_compound" and "MIP_compound" tasks
        task_id = _task_name_to_id("create_ome_zarr_compound", task_list)
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(args_non_parallel=dict(image_dir="/somewhere")),
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        EXPECTED_STATUSES[str(workflow_task_id)] = "done"
        task_id = _task_name_to_id("MIP_compound", task_list)
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json={},
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        EXPECTED_STATUSES[str(workflow_task_id)] = "done"
        # Add "generic_task" task
        task_id = _task_name_to_id("generic_task", task_list)
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(args_non_parallel=dict(raise_error=True)),
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        EXPECTED_STATUSES[str(workflow_task_id)] = "failed"

        # EXECUTE WORKFLOW
        res = await client.post(
            f"{PREFIX}/project/{project_id}/job/submit/"
            f"?workflow_id={workflow_id}&dataset_id={dataset_id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check job
        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        debug(job_status_data["working_dir"])
        assert job_status_data["log"]
        assert job_status_data["status"] == "failed"
        assert "ValueError" in job_status_data["log"]

        # The temporary output of the successful tasks must have been written
        # into the dataset filters&images attributes, and the history must
        # include both successful and failed tasks
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/"
        )
        assert res.status_code == 200
        dataset = res.json()
        EXPECTED_FILTERS = {
            "attributes": {},
            "types": {
                "3D": False,
            },
        }
        assert dataset["filters"] == EXPECTED_FILTERS
        assert len(dataset["history"]) == 3
        assert [item["status"] for item in dataset["history"]] == [
            "done",
            "done",
            "failed",
        ]
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/images/query/"
        )
        assert res.status_code == 200
        image_list = res.json()["images"]
        debug(image_list)
        assert len(image_list) == 4

        # Test get_workflowtask_status endpoint
        res = await client.get(
            (
                f"{PREFIX}/project/{project_id}/status/?"
                f"dataset_id={dataset_id}&workflow_id={workflow_id}"
            )
        )
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert statuses == EXPECTED_STATUSES


@pytest.mark.parametrize("backend", ["slurm"])
async def test_failing_workflow_JobExecutionError(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    backend,
    override_settings_factory,
    fractal_tasks_mock,
    tmp_path_factory,
    monkey_slurm,
    monkey_slurm_user,
    relink_python_interpreter_v2,
    tmp_path,
):
    # Use a session-scoped FRACTAL_TASKS_DIR folder
    basetemp = tmp_path_factory.getbasetemp()
    FRACTAL_TASKS_DIR = basetemp / "FRACTAL_TASKS_DIR"
    selected_new_settings = dict(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
        FRACTAL_TASKS_DIR=FRACTAL_TASKS_DIR,
    )
    if backend == "slurm":
        selected_new_settings.update(
            dict(FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json")
        )
    override_settings_factory(**selected_new_settings)

    user_cache_dir = str(tmp777_path / "user_cache_dir")
    user_kwargs = dict(cache_dir=user_cache_dir, is_verified=True)
    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        project = await project_factory_v2(user)
        project_id = project.id
        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="dataset",
        )
        dataset_id = dataset.id

        # Create workflow
        workflow = await workflow_factory_v2(
            name="test_wf", project_id=project_id
        )
        workflow_id = workflow.id

        # Retrieve relevant task ID
        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 200
        task_list = res.json()
        task_id = _task_name_to_id(
            task_name="generic_task", task_list=task_list
        )

        # Add a short task, which will be run successfully
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(args_non_parallel=dict(sleep_time=0.1)),
        )
        assert res.status_code == 201
        wftask0_id = res.json()["id"]

        # Add a long task, which will be stopped while running
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(args_non_parallel=dict(sleep_time=200)),
        )
        assert res.status_code == 201
        wftask1_id = res.json()["id"]

        # NOTE: the client.post call below is blocking, due to the way we are
        # running tests. For this reason, we call the scancel function from a
        # from a subprocess.Popen, so that we can make it happen during the
        # execution.
        scancel_sleep_time = 10
        slurm_user = monkey_slurm_user

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

        # Submit the workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/job/submit/"
            f"?{workflow_id=}&{dataset_id=}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202
        job_id = job_data["id"]
        debug(job_id)

        # Query status of the job
        rs = await client.get(f"{PREFIX}/project/{project_id}/job/{job_id}/")
        assert rs.status_code == 200
        job_status_data = rs.json()
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
            (
                f"{PREFIX}/project/{project_id}/status/?"
                f"dataset_id={dataset_id}&workflow_id={workflow_id}"
            )
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


@pytest.mark.parametrize("backend", backends_available)
async def test_non_executable_task_command(
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    task_factory_v2,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    backend,
    override_settings_factory,
    request,
):
    """
    Execute a workflow with a task which has an invalid `command` (i.e. it is
    not executable).
    """

    selected_new_settings = dict(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
    )
    if backend == "slurm":
        selected_new_settings.update(
            dict(FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json")
        )
    override_settings_factory(**selected_new_settings)

    debug(f"Testing with {backend=}")
    user_kwargs = {"is_verified": True}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v2")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_kwargs["cache_dir"] = user_cache_dir

    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        # Create task
        task = await task_factory_v2(
            name="invalid-task-command",
            source="some_source",
            type="non_parallel",
            command_non_parallel=str(testdata_path / "non_executable_task.sh"),
        )
        debug(task)

        # Create project
        project = await project_factory_v2(user)
        project_id = project.id

        # Create workflow
        workflow = await workflow_factory_v2(
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
        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="input",
        )
        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/job/submit/"
            f"?dataset_id={dataset.id}"
            f"&workflow_id={workflow.id}",
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


@pytest.mark.parametrize("backend", backends_available)
@pytest.mark.parametrize("legacy", [False, True])
async def test_failing_workflow_UnknownError(
    backend: str,
    legacy: bool,
    client,
    MockCurrentUser,
    testdata_path,
    tmp777_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    task_factory,
    request,
    override_settings_factory,
    monkeypatch,
    fractal_tasks_mock,  # see test docstring
):
    """
    Submit a workflow that fails with some unrecognized exception (due
    to a monkey-patched function in the runner).

    Note that the `fractal_tasks_mock` fixture is not needed here, but if we
    remove we hit another event-loop-related issue
    (https://github.com/fractal-analytics-platform/fractal-server/issues/1377).
    For the moment, we stick with this redundant side-effect.
    """
    EXPECTED_STATUSES = {}

    selected_new_settings = dict(
        FRACTAL_RUNNER_BACKEND=backend,
        FRACTAL_RUNNER_WORKING_BASE_DIR=tmp777_path / f"artifacts-{backend}",
    )
    if backend == "slurm":
        selected_new_settings.update(
            dict(FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json")
        )
    override_settings_factory(**selected_new_settings)

    debug(f"Testing with {backend=}")
    user_kwargs = {"is_verified": True}
    if backend == "slurm":
        request.getfixturevalue("monkey_slurm")
        request.getfixturevalue("relink_python_interpreter_v2")
        user_cache_dir = str(tmp777_path / f"user_cache_dir-{backend}")
        user_kwargs["cache_dir"] = user_cache_dir

    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
        project = await project_factory_v2(user)
        project_id = project.id
        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="dataset",
        )
        dataset_id = dataset.id
        workflow = await workflow_factory_v2(
            project_id=project_id, name="workflow"
        )
        workflow_id = workflow.id

        # Create task
        if legacy:
            task = await task_factory(command="echo", is_v2_compatible=True)
        else:
            task = await task_factory_v2(
                command_non_parallel="echo", type="non_parallel"
            )

        payload = dict(is_legacy_task=legacy)
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task.id}",
            json=payload,
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        EXPECTED_STATUSES[str(workflow_task_id)] = "failed"

        # Artificially introduce failure
        import fractal_server.app.runner.v2.runner

        ERROR_MSG = "This is the RuntimeError message."

        def _raise_RuntimeError(*args, **kwargs):
            raise RuntimeError(ERROR_MSG)

        if legacy:
            monkeypatch.setattr(
                fractal_server.app.runner.v2.runner,
                "run_v1_task_parallel",
                _raise_RuntimeError,
            )
        else:
            monkeypatch.setattr(
                fractal_server.app.runner.v2.runner,
                "run_v2_task_non_parallel",
                _raise_RuntimeError,
            )

        # EXECUTE WORKFLOW
        res = await client.post(
            f"{PREFIX}/project/{project_id}/job/submit/"
            f"?workflow_id={workflow_id}&dataset_id={dataset_id}",
            json={},
        )
        job_data = res.json()
        debug(job_data)
        assert res.status_code == 202

        # Check job
        res = await client.get(
            f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
        )
        assert res.status_code == 200
        job_status_data = res.json()
        debug(job_status_data)
        debug(job_status_data["working_dir"])
        assert job_status_data["log"]
        assert job_status_data["status"] == "failed"
        assert "UNKNOWN ERROR" in job_status_data["log"]
        assert ERROR_MSG in job_status_data["log"]

        # Test get_workflowtask_status endpoint
        res = await client.get(
            (
                f"{PREFIX}/project/{project_id}/status/?"
                f"dataset_id={dataset_id}&workflow_id={workflow_id}"
            )
        )
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        assert statuses == EXPECTED_STATUSES


# async def test_non_python_task(
#     client,
#     MockCurrentUser,
#     project_factory,
#     dataset_factory,
#     workflow_factory,
#     resource_factory,
#     task_factory,
#     testdata_path,
#     tmp_path,
# ):
#     """
#     Run a full workflow with a single bash task, which simply writes
#     something to stderr and stdout
#     """
#     async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
#         # Create project
#         project = await project_factory(user)
#         project_id = project.id

#         # Create workflow
#         workflow = await workflow_factory(
#             name="test_wf", project_id=project_id
#         )

#         # Create task
#         task = await task_factory(
#             name="non-python",
#             source="custom-task",
#             command=f"bash {str(testdata_path)}/non_python_task_issue189.sh",
#             input_type="zarr",
#             output_type="zarr",
#         )

#         # Add task to workflow
#         res = await client.post(
#             f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
#             f"?task_id={task.id}",
#             json=dict(),
#         )
#         assert res.status_code == 201

#         # Create datasets
#         dataset = await dataset_factory(
#             project_id=project_id, name="dataset", type="zarr",
#         )
#         await resource_factory(path=str(tmp_path / "data"), dataset=dataset)

#         # Submit workflow
#         res = await client.post(
#             f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
#             f"?input_dataset_id={dataset.id}"
#             f"&output_dataset_id={dataset.id}",
#             json={},
#         )
#         job_data = res.json()
#         debug(job_data)
#         assert res.status_code == 202

#         # Check that the workflow execution is complete
#         res = await client.get(
#             f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
#         )
#         assert res.status_code == 200
#         job_status_data = res.json()
#         debug(job_status_data)
#         assert job_status_data["status"] == "done"
#         debug(job_status_data["end_timestamp"])
#         assert job_status_data["end_timestamp"]

#         # Check that the expected files are present
#         working_dir = job_status_data["working_dir"]
#         glob_list = [Path(x).name for x in glob.glob(f"{working_dir}/*")]
#         must_exist = [
#             "0.args.json",
#             "0.err",
#             "0.metadiff.json",
#             "0.out",
#             WORKFLOW_LOG_FILENAME,
#         ]
#         for f in must_exist:
#             assert f in glob_list

#         # Check that stderr and stdout are as expected
#         with open(f"{working_dir}/0.out", "r") as f:
#             out = f.read()
#         assert "This goes to standard output" in out
#         with open(f"{working_dir}/0.err", "r") as f:
#             err = f.read()
#         assert "This goes to standard error" in err


# async def test_metadiff(
#     client,
#     MockCurrentUser,
#     project_factory,
#     dataset_factory,
#     workflow_factory,
#     resource_factory,
#     task_factory,
#     testdata_path,
#     tmp_path,
# ):
#     """
#     Run task with command which does not produce metadiff files, or which
#     produces a single `null` value rather than a dictionary. See issues 854
#     and 878.
#     """
#     task_file = str(testdata_path / "echo_sleep_task.sh")
#     task_file2 = str(testdata_path / "non_python_task_issue878.sh")
#     command = f"bash {task_file}"
#     command_null = f"bash {task_file2}"
#     async with MockCurrentUser(user_kwargs=dict(is_verified=True)) as user:
#         task0 = await task_factory(
#             name="task0",
#             source="task0",
#             command=command,
#             input_type="Any",
#             output_type="Any",
#         )
#         task1 = await task_factory(
#             name="task1",
#             source="task1",
#             command=command,
#             input_type="Any",
#             output_type="Any",
#             meta=dict(parallelization_level="index"),
#         )
#         task2 = await task_factory(
#             name="task2",
#             source="task2",
#             command=command_null,
#             input_type="Any",
#             output_type="Any",
#         )
#         task3 = await task_factory(
#             name="task3",
#             source="task3",
#             command=command_null,
#             input_type="Any",
#             output_type="Any",
#             meta=dict(parallelization_level="index"),
#         )

#         project = await project_factory(user)
#         project_id = project.id
#         workflow = await workflow_factory(
#             name="test_wf", project_id=project_id
#         )
#         for task in (task0, task1, task2, task3):
#             res = await client.post(
#                 f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
#                 f"?task_id={task.id}",
#                 json=dict(),
#             )
#             assert res.status_code == 201

#         dataset = await dataset_factory(
#             project_id=project_id,
#             name="dataset",
#             type="zarr",
#             meta=dict(index=["A", "B"]),
#         )
#         await resource_factory(path=str(tmp_path / "data"), dataset=dataset)
#         # Submit workflow
#         res = await client.post(
#             f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/apply/"
#             f"?input_dataset_id={dataset.id}"
#             f"&output_dataset_id={dataset.id}",
#             json={},
#         )
#         job_data = res.json()
#         debug(job_data)
#         assert res.status_code == 202

#         # Check that the workflow execution is complete
#         res = await client.get(
#             f"{PREFIX}/project/{project_id}/job/{job_data['id']}/"
#         )
#         assert res.status_code == 200
#         job_status_data = res.json()
#         debug(job_status_data)
#         assert job_status_data["status"] == "done"
#         debug(job_status_data["end_timestamp"])
#         assert job_status_data["end_timestamp"]

#         # Check that the expected files are present
#         working_dir = job_status_data["working_dir"]
#         glob_list = [Path(x).name for x in glob.glob(f"{working_dir}/*")]
#         debug(glob_list)
#         must_exist = [
#             "0.args.json",
#             "0.err",
#             "0.out",
#             "1_par_A.args.json",
#             "1_par_A.err",
#             "1_par_A.out",
#             "1_par_B.args.json",
#             "1_par_B.err",
#             "1_par_B.out",
#             "2.args.json",
#             "2.err",
#             "2.out",
#             "2.metadiff.json",
#             "3_par_A.args.json",
#             "3_par_A.err",
#             "3_par_A.out",
#             "3_par_B.args.json",
#             "3_par_B.err",
#             "3_par_B.out",
#             "3_par_A.metadiff.json",
#             "3_par_B.metadiff.json",
#             WORKFLOW_LOG_FILENAME,
#         ]
#         for f in must_exist:
#             assert f in glob_list

#         # Check that workflow.log includes expected warnings
#         with open(f"{working_dir}/{WORKFLOW_LOG_FILENAME}", "r") as f:
#             logs = f.read()
#         print(logs)
#         assert "Skip collection of updated metadata" in logs
