import os
import shutil
import zipfile
from contextlib import contextmanager
from pathlib import Path

from devtools import debug

from fractal_server.app.models.v2 import TaskV2
from fractal_server.runner.filenames import WORKFLOW_LOG_FILENAME


@contextmanager
def informative_assertion_block(*args):
    try:
        yield
    except AssertionError as e:
        debug("SOME ASSERTION FAILED")
        for arg in args:
            debug(arg)
        raise e


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
    resource_id: int,
    user_kwargs: dict | None = None,
):
    if user_kwargs is None:
        user_kwargs = {}

    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs}
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
        wftask0_id = res.json()["id"]
        debug(wftask0_id)

        # Add "MIP_compound" task
        task_id_B = tasks["MIP_compound"].id
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id_B}",
            json={},
        )
        assert res.status_code == 201
        wftask1_id = res.json()["id"]
        debug(wftask1_id)

        # Add "generic_task_parallel" task
        task_id_C = tasks["generic_task_parallel"].id
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id_C}",
            json={},
        )
        assert res.status_code == 201
        wftask2_id = res.json()["id"]
        debug(wftask2_id)

        # EXECUTE WORKFLOW
        # Note: we include a fake `worker_init` in order to catch the bug
        # in issue 2659
        res = await client.post(
            f"{PREFIX}/project/{project_id}/job/submit/"
            f"?workflow_id={workflow_id}&dataset_id={dataset_id}",
            json={
                "worker_init": "# this is a fake extra line, with no meaning"
            },
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
        with informative_assertion_block(job_status_data):
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

        # Check output images
        res = await client.post(
            (
                f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/"
                "images/query/"
            ),
            json={},
        )
        assert res.status_code == 200
        image_page = res.json()
        debug(image_page)
        # There should be NUM_IMAGES 3D images and NUM_IMAGES 2D images
        assert image_page["total_count"] == 2 * NUM_IMAGES
        images = image_page["items"]
        debug(images)
        images_3D = filter(lambda img: img["types"]["3D"], images)
        images_2D = filter(lambda img: not img["types"]["3D"], images)
        assert len(list(images_2D)) == NUM_IMAGES
        assert len(list(images_3D)) == NUM_IMAGES

        # Check files in zipped root job folder
        working_dir = job_status_data["working_dir"]
        with zipfile.ZipFile(f"{working_dir}.zip", "r") as zip_ref:
            all_actual_files = zip_ref.namelist()
            assert WORKFLOW_LOG_FILENAME in all_actual_files

        # Check files in task-0 folder
        expected_files = {
            "non_par--log.txt",
            "non_par--metadiff.json",
            "par-000000-0000000-log.txt",
            "par-000000-0000000-metadiff.json",
        }
        task_actual_files = {
            file.split("/")[-1]
            for file in all_actual_files
            if "0_create_ome_zarr_compound" in file
        }
        with informative_assertion_block(
            expected_files,
            all_actual_files,
            task_actual_files,
        ):
            assert expected_files < task_actual_files

        # Check files in task-1 folder
        expected_files = {
            "non_par--log.txt",
            "non_par--metadiff.json",
            "par-000000-0000000-log.txt",
            "par-000000-0000000-metadiff.json",
        }
        task_actual_files = {
            file.split("/")[-1]
            for file in all_actual_files
            if "1_mip_compound" in file
        }
        with informative_assertion_block(
            expected_files,
            all_actual_files,
            task_actual_files,
        ):
            assert expected_files < task_actual_files

        # GET dataset history
        url = f"api/v2/project/{project_id}/dataset/{dataset_id}/history/"
        res = await client.get(url)
        assert res.status_code == 200
        assert len(res.json()) == 3
        for item in res.json():
            assert "workflowtask_dump" in item.keys()
            assert "task_group_dump" in item.keys()

        # GET workflow status
        url = (
            f"api/v2/project/{project_id}/status/"
            f"?dataset_id={dataset_id}&workflow_id={workflow_id}"
        )
        res = await client.get(url)
        assert res.status_code == 200
        assert res.json() == {
            # Converter compound task
            "1": {
                "status": "done",
                "num_available_images": 0,
                "num_submitted_images": 0,
                "num_done_images": 0,
                "num_failed_images": 0,
            },
            # MIP compound task
            "2": {
                "status": "done",
                "num_available_images": 4,
                "num_submitted_images": 0,
                "num_done_images": 4,
                "num_failed_images": 0,
            },
            # Generic parallel task
            "3": {
                "status": "done",
                "num_available_images": 4,
                "num_submitted_images": 0,
                "num_done_images": 4,
                "num_failed_images": 0,
            },
        }

        for wftask_id in [wftask0_id, wftask1_id, wftask2_id]:
            # GET history runs
            query_wft = f"dataset_id={dataset_id}&workflowtask_id={wftask_id}"
            this_prefix = f"api/v2/project/{project_id}/status"
            url = f"{this_prefix}/run/?{query_wft}"
            res = await client.get(url)
            assert res.status_code == 200
            assert len(res.json()) == 1
            history_run_id = res.json()[0]["id"]
            debug(res.json())

            # Get history units
            url = f"{this_prefix}/run/{history_run_id}/units/?{query_wft}"
            res = await client.get(url)
            assert res.status_code == 200
            debug(res.json())
            if wftask_id == wftask0_id:
                # Converter compound task
                assert res.json()["total_count"] == NUM_IMAGES + 1
            elif wftask_id == wftask1_id:
                # MIP compound task
                assert res.json()["total_count"] == NUM_IMAGES + 1
            elif wftask_id == wftask2_id:
                # Generic parallel task
                assert res.json()["total_count"] == NUM_IMAGES

            first_history_unit = res.json()["items"][0]
            history_unit_id = first_history_unit["id"]
            assert Path(first_history_unit["logfile"]).exists()
            debug(first_history_unit["logfile"])

            # Get history-unit log
            url = (
                f"{this_prefix}/unit-log/?"
                f"history_run_id={history_run_id}&"
                f"history_unit_id={history_unit_id}&"
                f"workflowtask_id={wftask_id}&dataset_id={dataset_id}"
            )
            res = await client.get(url)
            assert res.status_code == 200
            assert "not available" not in res.json()


async def full_workflow_TaskExecutionError(
    *,
    MockCurrentUser,
    client,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    tasks: dict[str, TaskV2],
    resource_id: int,
    user_kwargs: dict | None = None,
):
    if user_kwargs is None:
        user_kwargs = {}

    EXPECTED_STATUSES = {}
    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs}
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
        with informative_assertion_block(job_status_data):
            assert job_status_data["log"]
            assert job_status_data["status"] == "failed"
            assert "ValueError" in job_status_data["log"]

        # The temporary output of the successful tasks must have been written
        # into the dataset images
        res = await client.get(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/"
        )
        assert res.status_code == 200
        dataset = res.json()
        res = await client.post(
            f"{PREFIX}/project/{project_id}/dataset/{dataset_id}/images/query/"
        )
        assert res.status_code == 200
        image_list = res.json()["items"]
        debug(image_list)
        assert len(image_list) == 2 * NUM_IMAGES


async def non_executable_task_command(
    *,
    MockCurrentUser,
    client,
    testdata_path,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    resource_id: int,
    user_kwargs: dict | None = None,
):
    if user_kwargs is None:
        user_kwargs = {}

    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs},
    ) as user:
        # Create task
        task = await task_factory_v2(
            user_id=user.id,
            name="invalid-task-command",
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
            zarr_dir="/fake",
            images=[dict(zarr_url="/fake/1")],
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
    task_factory_v2,
    resource_id: int,
    user_kwargs: dict | None = None,
):
    if user_kwargs is None:
        user_kwargs = {}

    EXPECTED_STATUSES = {}
    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs}
    ) as user:
        project = await project_factory_v2(user)
        project_id = project.id
        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="dataset",
            zarr_dir="/fake",
            images=[dict(zarr_url="/fake/1")],
        )
        dataset_id = dataset.id
        workflow = await workflow_factory_v2(
            project_id=project_id, name="workflow"
        )
        workflow_id = workflow.id

        # Create task
        task = await task_factory_v2(
            user_id=user.id,
            command_non_parallel="echo",
            type="non_parallel",
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
        import fractal_server.runner.v2.runner

        ERROR_MSG = "This is the RuntimeError message."

        def _raise_RuntimeError(*args, **kwargs):
            raise RuntimeError(ERROR_MSG)

        monkeypatch.setattr(
            fractal_server.runner.v2.runner,
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
        assert "JOB ERROR" in job_status_data["log"]
        assert ERROR_MSG in job_status_data["log"]

        # GET workflow status and assert that there is no "submitted"
        url = (
            f"api/v2/project/{project_id}/status/"
            f"?dataset_id={dataset_id}&workflow_id={workflow_id}"
        )
        res = await client.get(url)
        assert res.status_code == 200
        debug(res.json())
        assert res.json() == {
            f"{workflow_task_id}": {
                "status": "failed",
                "num_available_images": 1,
                "num_submitted_images": 0,
                "num_done_images": 0,
                "num_failed_images": 0,
            },
        }


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
    resource_id: int,
    additional_user_kwargs=None,
    this_should_fail: bool = False,
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

    async with MockCurrentUser(user_kwargs=user_kwargs) as user:
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
            project_id=project_id,
            name="dataset",
            zarr_dir="/fake",
            images=[dict(zarr_url="/fake/1")],
        )

        # Submit workflow
        res = await client.post(
            f"{PREFIX}/project/{project.id}/job/submit/"
            f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
            json={},
        )
        job_data = res.json()
        with informative_assertion_block(job_data):
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
            actual_files = zip_ref.namelist()
        with informative_assertion_block(actual_files):
            assert WORKFLOW_LOG_FILENAME in actual_files
            assert "0_non_python/non_par--args.json" in actual_files
            assert "0_non_python/non_par--log.txt" in actual_files

        # Check that stderr and stdout are as expected
        with zipfile.ZipFile(f"{working_dir}.zip", "r") as zip_ref:
            with zip_ref.open("0_non_python/non_par--log.txt", "r") as file:
                log = file.read().decode("utf-8")
        assert "This goes to standard output" in log
        assert "This goes to standard error" in log

        return job_status_data["log"]


async def failing_workflow_post_task_execution(
    *,
    MockCurrentUser,
    client,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    tasks: dict[str, TaskV2],
    resource_id: int,
    user_kwargs: dict | None = None,
    tmp_path: Path,
):
    if user_kwargs is None:
        user_kwargs = {}

    async with MockCurrentUser(
        user_kwargs={"is_verified": True, **user_kwargs},
    ) as user:
        project = await project_factory_v2(user)
        project_id = project.id

        zarr_dir = (tmp_path / "zarr_dir").as_posix().rstrip("/")

        dataset = await dataset_factory_v2(
            project_id=project_id,
            name="dataset",
            zarr_dir=zarr_dir,
            images=[
                dict(zarr_url=Path(zarr_dir, str(index)).as_posix())
                for index in range(2)
            ],
        )
        dataset_id = dataset.id
        workflow = await workflow_factory_v2(
            project_id=project_id, name="workflow"
        )
        workflow_id = workflow.id

        task_id = tasks["dummy_remove_images"].id
        res = await client.post(
            f"{PREFIX}/project/{project_id}/workflow/{workflow_id}/wftask/"
            f"?task_id={task_id}",
            json=dict(
                args_non_parallel=dict(
                    more_zarr_urls=[Path(zarr_dir, "missing-image").as_posix()]
                ),
            ),
        )
        assert res.status_code == 201
        wftask_id = res.json()["id"]
        debug(wftask_id)

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
        assert job_status_data["log"]
        debug(job_status_data["working_dir"])
        with informative_assertion_block(job_status_data):
            assert job_status_data["status"] == "failed"

        # GET workflow status
        url = (
            f"api/v2/project/{project_id}/status/"
            f"?dataset_id={dataset_id}&workflow_id={workflow_id}"
        )
        res = await client.get(url)
        assert res.status_code == 200
        debug(res.json())
        assert res.json() == {
            str(wftask_id): {
                "status": "submitted",
                "num_available_images": 2,
                "num_submitted_images": 0,
                "num_done_images": 0,
                "num_failed_images": 2,
            },
        }

        # GET history runs
        query_wft = f"dataset_id={dataset_id}&workflowtask_id={wftask_id}"
        this_prefix = f"api/v2/project/{project_id}/status"
        url = f"{this_prefix}/run/?{query_wft}"
        res = await client.get(url)
        assert res.status_code == 200
        assert len(res.json()) == 1
        history_run_id = res.json()[0]["id"]
        debug(res.json())
        assert res.json()[0]["num_submitted_units"] == 0
        assert res.json()[0]["num_done_units"] == 0
        assert res.json()[0]["num_failed_units"] == 1

        # Get history units
        url = f"{this_prefix}/run/{history_run_id}/units/?{query_wft}"
        res = await client.get(url)
        assert res.status_code == 200
        debug(res.json())
        first_history_unit = res.json()["items"][0]
        assert first_history_unit["status"] == "failed"
