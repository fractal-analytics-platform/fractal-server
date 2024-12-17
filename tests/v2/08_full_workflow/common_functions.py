import os
import shutil
import zipfile
from pathlib import Path
from typing import Optional

from devtools import debug

from fractal_server.app.models.v2 import TaskV2
from fractal_server.app.runner.filenames import FILTERS_FILENAME
from fractal_server.app.runner.filenames import HISTORY_FILENAME
from fractal_server.app.runner.filenames import IMAGES_FILENAME
from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME

PREFIX = "/api/v2"
NUM_IMAGES = 4


async def full_workflow(
    *,
    MockCurrentUser,
    client,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    tasks: dict[str, TaskV2],
    user_kwargs: Optional[dict] = None,
    user_settings_dict: Optional[dict] = None,
):
    if user_kwargs is None:
        user_kwargs = {}

    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs},
        user_settings_dict=user_settings_dict,
    ) as user:
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

        # Add "create_ome_zarr_compound" task
        task_id_A = tasks["create_ome_zarr_compound"].id
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id_A}",
            json=dict(
                args_non_parallel=dict(
                    image_dir="/somewhere", num_images=NUM_IMAGES
                )
            ),
        )
        assert res.status_code == 201
        # Add "MIP_compound" task
        task_id_B = tasks["MIP_compound"].id
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
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/"
            "images/query/",
            json={},
        )
        assert res.status_code == 200
        image_page = res.json()
        debug(image_page)
        # There should be NUM_IMAGES 3D images and NUM_IMAGES 2D images
        assert image_page["total_count"] == 2 * NUM_IMAGES
        images = image_page["images"]
        debug(images)
        images_3D = filter(lambda img: img["types"]["3D"], images)
        images_2D = filter(lambda img: not img["types"]["3D"], images)
        assert len(list(images_2D)) == NUM_IMAGES
        assert len(list(images_3D)) == NUM_IMAGES

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

        # Check files in zipped root job folder
        working_dir = job_status_data["working_dir"]
        with zipfile.ZipFile(f"{working_dir}.zip", "r") as zip_ref:
            actual_files = zip_ref.namelist()
        expected_files = [
            HISTORY_FILENAME,
            FILTERS_FILENAME,
            IMAGES_FILENAME,
            WORKFLOW_LOG_FILENAME,
        ]
        assert set(expected_files) < set(actual_files)

        # Check files in task-0 folder
        expected_files = ["0_par_0000000.log", "0_par_0000001.log"]
        assert set(expected_files) < set(
            file.split("/")[-1]
            for file in actual_files
            if "0_create_ome_zarr_compound" in file
        )

        # Check files in task-1 folder
        expected_files = ["1_par_0000000.log", "1_par_0000001.log"]
        assert set(expected_files) < set(
            file.split("/")[-1]
            for file in actual_files
            if "1_mip_compound" in file
        )


async def full_workflow_TaskExecutionError(
    *,
    MockCurrentUser,
    client,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    tasks: dict[str, TaskV2],
    user_kwargs: Optional[dict] = None,
    user_settings_dict: Optional[dict] = None,
):

    if user_kwargs is None:
        user_kwargs = {}

    EXPECTED_STATUSES = {}
    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs},
        user_settings_dict=user_settings_dict,
    ) as user:
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

        # Add "create_ome_zarr_compound" and "MIP_compound" tasks
        task_id = tasks["create_ome_zarr_compound"].id
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(
                args_non_parallel=dict(
                    image_dir="/somewhere", num_images=NUM_IMAGES
                )
            ),
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        EXPECTED_STATUSES[str(workflow_task_id)] = "done"
        task_id = tasks["MIP_compound"].id
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json={},
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        EXPECTED_STATUSES[str(workflow_task_id)] = "done"
        # Add "generic_task" task
        task_id = tasks["generic_task"].id
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
            "attributes_include": {},
            "attributes_exclude": {},
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
        assert len(image_list) == 2 * NUM_IMAGES

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


async def non_executable_task_command(
    *,
    MockCurrentUser,
    client,
    testdata_path,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    user_kwargs: Optional[dict] = None,
    user_settings_dict: Optional[dict] = None,
):
    if user_kwargs is None:
        user_kwargs = {}

    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs},
        user_settings_dict=user_settings_dict,
    ) as user:
        # Create task
        task = await task_factory_v2(
            user_id=user.id,
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


async def failing_workflow_UnknownError(
    *,
    MockCurrentUser,
    client,
    monkeypatch,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory,
    task_factory_v2,
    user_kwargs: Optional[dict] = None,
    user_settings_dict: Optional[dict] = None,
):
    if user_kwargs is None:
        user_kwargs = {}

    EXPECTED_STATUSES = {}
    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs},
        user_settings_dict=user_settings_dict,
    ) as user:
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
        task = await task_factory_v2(
            user_id=user.id, command_non_parallel="echo", type="non_parallel"
        )

        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task.id}",
            json={},
        )
        assert res.status_code == 201
        workflow_task_id = res.json()["id"]
        EXPECTED_STATUSES[str(workflow_task_id)] = "failed"

        # Artificially introduce failure
        import fractal_server.app.runner.v2.runner

        ERROR_MSG = "This is the RuntimeError message."

        def _raise_RuntimeError(*args, **kwargs):
            raise RuntimeError(ERROR_MSG)

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


async def workflow_with_non_python_task(
    *,
    MockCurrentUser,
    client,
    testdata_path,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    tmp777_path: Path,
    additional_user_kwargs=None,
    this_should_fail: bool = False,
    user_settings_dict: Optional[dict] = None,
) -> str:
    """
    Run a non-python-task Fractal job.

    Returns:
        String with job logs.
    """

    user_kwargs = {"is_verified": True}
    if additional_user_kwargs is not None:
        user_kwargs.update(additional_user_kwargs)
    debug(user_kwargs)

    async with MockCurrentUser(
        user_kwargs=user_kwargs, user_settings_dict=user_settings_dict
    ) as user:
        # Create project
        project = await project_factory_v2(user)
        project_id = project.id

        # Create workflow
        workflow = await workflow_factory_v2(
            name="test_wf", project_id=project_id
        )

        # Copy script somewhere accessible
        script_name = "non_python_task_issue1377.sh"
        script_path = tmp777_path / script_name
        shutil.copy(
            testdata_path / script_name,
            script_path,
        )

        # Create task
        task = await task_factory_v2(
            user_id=user.id,
            name="non-python",
            source="custom-task",
            type="non_parallel",
            command_non_parallel=(f"bash {script_path.as_posix()}"),
        )

        # Add task to workflow
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow.id}/wftask/"
            f"?task_id={task.id}",
            json=dict(),
        )
        assert res.status_code == 201

        # Create datasets
        dataset = await dataset_factory_v2(
            project_id=project_id, name="dataset"
        )

        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
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

        if this_should_fail:
            assert job_status_data["status"] == "failed"
            return job_status_data["log"]

        assert job_status_data["status"] == "done"
        debug(job_status_data["end_timestamp"])
        assert job_status_data["end_timestamp"]

        # Check that the expected files are present
        working_dir = job_status_data["working_dir"]
        with zipfile.ZipFile(f"{working_dir}.zip", "r") as zip_ref:
            glob_list = [name.split("/")[-1] for name in zip_ref.namelist()]

        must_exist = [
            "0.log",
            "0.args.json",
            IMAGES_FILENAME,
            HISTORY_FILENAME,
            FILTERS_FILENAME,
            WORKFLOW_LOG_FILENAME,
        ]

        for f in must_exist:
            if f not in glob_list:
                raise ValueError(f"{f} must exist, but {glob_list=}")

        # Check that stderr and stdout are as expected
        with zipfile.ZipFile(f"{working_dir}.zip", "r") as zip_ref:
            with zip_ref.open("0_non_python/0.log", "r") as file:
                log = file.read().decode("utf-8")
        assert "This goes to standard output" in log
        assert "This goes to standard error" in log

        return job_status_data["log"]
