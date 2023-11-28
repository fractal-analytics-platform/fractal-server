from pathlib import Path
from zipfile import ZipFile

import pytest
from devtools import debug

from fractal_server.app.runner import _backends
from fractal_server.app.runner._common import SHUTDOWN_FILENAME
from fractal_server.app.schemas import JobStatusType


PREFIX = "/api/v1"

backends_available = list(_backends.keys())


async def test_patch_job(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    task_factory,
    client,
    registered_superuser_client,
    db,
    tmp_path,
):
    ORIGINAL_STATUS = JobStatusType.SUBMITTED
    NEW_STATUS = JobStatusType.FAILED

    async with MockCurrentUser(user_kwargs={"id": 111111}) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="source")
        await workflow.insert_task(task_id=task.id, db=db)
        dataset = await dataset_factory(project_id=project.id)
        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            workflow_id=workflow.id,
            status=ORIGINAL_STATUS,
        )
        # Read job as job owner (standard user)
        res = await client.get(f"{PREFIX}/project/{project.id}/job/{job.id}/")
        assert res.status_code == 200
        assert res.json()["status"] == ORIGINAL_STATUS
        # Patch job as job owner (standard user) and fail
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/job/{job.id}/",
            json={"status": NEW_STATUS},
        )
        assert res.status_code == 403
        # Patch job as superuser
        async with MockCurrentUser(
            user_kwargs={"id": 222222, "is_superuser": True}
        ):
            # Fail due to invalid payload (missing attribute "status")
            res = await registered_superuser_client.patch(
                f"{PREFIX}/project/{project.id}/job/{job.id}/",
                json={"working_dir": "/tmp"},
            )
            assert res.status_code == 422
            # Fail due to invalid payload (status not part of JobStatusType)
            res = await registered_superuser_client.patch(
                f"{PREFIX}/project/{project.id}/job/{job.id}/",
                json={"status": "something_invalid"},
            )
            assert res.status_code == 422
            # Fail due to non-existing job
            res = await registered_superuser_client.patch(
                f"{PREFIX}/project/{project.id}/job/{123456789}/",
                json={"status": NEW_STATUS},
            )
            assert res.status_code == 404
            # Successfully apply patch
            res = await registered_superuser_client.patch(
                f"{PREFIX}/project/{project.id}/job/{job.id}/",
                json={"status": NEW_STATUS},
            )
            assert res.status_code == 200
            assert res.json()["status"] == NEW_STATUS
        # Read job as job owner (standard user)
        res = await client.get(f"{PREFIX}/project/{project.id}/job/{job.id}/")
        assert res.status_code == 200
        assert res.json()["status"] == NEW_STATUS


@pytest.mark.parametrize("backend", backends_available)
async def test_stop_job(
    backend,
    db,
    client,
    MockCurrentUser,
    project_factory,
    job_factory,
    workflow_factory,
    dataset_factory,
    tmp_path,
    task_factory,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=backend)

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        wf = await workflow_factory(project_id=project.id)
        t = await task_factory(name="task", source="source")
        ds = await dataset_factory(project_id=project.id)
        await wf.insert_task(task_id=t.id, db=db)
        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=ds.id,
            output_dataset_id=ds.id,
            workflow_id=wf.id,
        )

        debug(job)

        res = await client.get(
            f"api/v1/project/{project.id}/job/{job.id}/stop/"
        )
        if backend == "slurm":
            assert res.status_code == 204

            shutdown_file = tmp_path / SHUTDOWN_FILENAME
            debug(shutdown_file)
            assert shutdown_file.exists()
        else:
            assert res.status_code == 422


async def test_job_list(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    task_factory,
    tmp_path,
    db,
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

        # Test that the endpoint returns an empty job list
        res = await client.get(f"{PREFIX}/project/{prj.id}/job/")
        assert res.status_code == 200
        debug(res.json())
        assert len(res.json()) == 0

        # Create all needed objects in the database
        input_dataset = await dataset_factory(project_id=prj.id, name="input")
        output_dataset = await dataset_factory(
            project_id=prj.id, name="output"
        )
        workflow = await workflow_factory(project_id=prj.id)
        t = await task_factory()
        await workflow.insert_task(task_id=t.id, db=db)
        job = await job_factory(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset.id,
        )
        debug(job)

        # Test that the endpoint returns a list with the new job
        res = await client.get(f"{PREFIX}/project/{prj.id}/job/")
        assert res.status_code == 200
        debug(res.json())
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == job.id


async def test_job_download_logs(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    task_factory,
    db,
    tmp_path,
):
    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

        # Create all needed objects in the database
        input_dataset = await dataset_factory(project_id=prj.id, name="input")
        output_dataset = await dataset_factory(
            project_id=prj.id, name="output"
        )
        workflow = await workflow_factory(project_id=prj.id)
        t = await task_factory()
        await workflow.insert_task(task_id=t.id, db=db)
        working_dir = (tmp_path / "workflow_dir_for_zipping").as_posix()
        job = await job_factory(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=working_dir,
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset.id,
        )
        debug(job)

        # Write a log file in working_dir
        LOG_CONTENT = "This is a log\n"
        LOG_FILE = "log.txt"
        Path(working_dir).mkdir()
        with (Path(working_dir) / LOG_FILE).open("w") as f:
            f.write(LOG_CONTENT)

        # Test that the endpoint returns a list with the new job
        res = await client.get(
            f"{PREFIX}/project/{prj.id}/job/{job.id}/download/"
        )
        assert res.status_code == 200
        assert (
            res.headers.get("content-type") == "application/x-zip-compressed"
        )

        # Write response into a zipped file
        zipped_archive_path = tmp_path / "logs.zip"
        debug(zipped_archive_path)
        with zipped_archive_path.open("wb") as f:
            f.write(res.content)

        # Unzip the log archive
        unzipped_archived_path = tmp_path / "unzipped_logs"
        debug(unzipped_archived_path)
        with ZipFile(zipped_archive_path, mode="r") as zipfile:
            zipfile.extractall(path=unzipped_archived_path)

        # Verify content of the unzipped log archive
        with (unzipped_archived_path / LOG_FILE).open("r") as f:
            actual_logs = f.read()
        assert LOG_CONTENT in actual_logs


async def test_get_job_list(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    db,
    task_factory,
    client,
    tmp_path,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)

        res = await client.get(f"{PREFIX}/project/{project.id}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 0

        workflow = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)

        t = await task_factory()
        await workflow.insert_task(task_id=t.id, db=db)
        dataset = await dataset_factory(project_id=project.id)

        N = 5
        for i in range(N):
            await job_factory(
                project_id=project.id,
                input_dataset_id=dataset.id,
                output_dataset_id=dataset.id,
                workflow_id=workflow.id,
                working_dir=tmp_path,
            )

        res = await client.get(f"{PREFIX}/project/{project.id}/job/")
        debug(res)
        assert res.status_code == 200
        assert len(res.json()) == N

        res = await client.get(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/job/"
        )
        assert res.status_code == 200
        assert len(res.json()) == N
        res = await client.get(
            f"{PREFIX}/project/{project.id}/workflow/{workflow2.id}/job/"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0


async def test_get_user_jobs(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    job_factory,
    db,
    client,
    tmp_path,
):

    async with MockCurrentUser(persist=True, user_kwargs={"id": 123}) as user:

        task = await task_factory()

        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        await workflow.insert_task(task_id=task.id, db=db)
        dataset = await dataset_factory(project_id=project.id)

        for _ in range(3):
            await job_factory(
                working_dir=tmp_path.as_posix(),
                project_id=project.id,
                input_dataset_id=dataset.id,
                output_dataset_id=dataset.id,
                workflow_id=workflow.id,
            )

        project2 = await project_factory(user)
        workflow2 = await workflow_factory(project_id=project.id)
        await workflow2.insert_task(task_id=task.id, db=db)
        dataset2 = await dataset_factory(project_id=project.id)

        for _ in range(2):
            await job_factory(
                working_dir=tmp_path.as_posix(),
                project_id=project2.id,
                input_dataset_id=dataset2.id,
                output_dataset_id=dataset2.id,
                workflow_id=workflow2.id,
            )

        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 5

    async with MockCurrentUser(persist=True, user_kwargs={"id": 321}):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 0
