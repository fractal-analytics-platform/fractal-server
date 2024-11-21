from datetime import datetime
from datetime import timezone
from pathlib import Path
from urllib.parse import quote
from zipfile import ZipFile

import pytest
from devtools import debug

from fractal_server.app.models.v1 import JobStatusTypeV1
from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.app.runner.v1 import _backends

backends_available = list(_backends.keys())

PREFIX = "/admin/v1"


async def test_unauthorized_to_admin(client, MockCurrentUser):

    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 401
        res = await client.get(f"{PREFIX}/workflow/")
        assert res.status_code == 401
        res = await client.get(f"{PREFIX}/dataset/")
        assert res.status_code == 401
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/workflow/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/dataset/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200


async def test_view_project(client, MockCurrentUser, project_factory):

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True}
    ) as superuser:
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert res.json() == []
        await project_factory(superuser)

    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:
        project = await project_factory(user)
        prj_id = project.id
        project_2_timestamp = project.timestamp_created.isoformat()
        await project_factory(user)
        user_id = user.id

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert len(res.json()) == 3
        res = await client.get(f"{PREFIX}/project/?id={prj_id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/?user_id={user_id}")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/project/?user_id={user_id}&id={prj_id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(
            f"{PREFIX}/project/?user_id={user_id}&id={prj_id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1

        res = await client.get(
            f"{PREFIX}/project/"
            f"?timestamp_created_min={quote(project_2_timestamp)}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2

        res = await client.get(
            f"{PREFIX}/project/"
            f"?timestamp_created_max={quote(project_2_timestamp)}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2


async def test_view_workflow(
    client, MockCurrentUser, project_factory, workflow_factory
):

    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:

        project1 = await project_factory(user)
        workflow1a = await workflow_factory(
            project_id=project1.id, name="Workflow 1a"
        )
        workflow1b = await workflow_factory(
            project_id=project1.id, name="Workflow 1b"
        )

        project2 = await project_factory(user)
        workflow2a = await workflow_factory(
            project_id=project2.id, name="Workflow 2a"
        )

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True}
    ) as superuser:
        project3 = await project_factory(superuser)
        await workflow_factory(project_id=project3.id, name="super")

        # get all workflows
        res = await client.get(f"{PREFIX}/workflow/")
        assert res.status_code == 200
        assert len(res.json()) == 4

        # get workflows by user_id
        res = await client.get(f"{PREFIX}/workflow/?user_id={user.id}")
        assert res.status_code == 200
        assert len(res.json()) == 3

        # get workflows by id
        res = await client.get(
            f"{PREFIX}/workflow/?user_id={user.id}&id={workflow1a.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow1a.name

        # get workflows by project_id
        res = await client.get(f"{PREFIX}/workflow/?project_id={project1.id}")
        assert res.status_code == 200
        assert len(res.json()) == 2

        # get workflows by project_id and id
        res = await client.get(
            f"{PREFIX}/workflow/"
            f"?project_id={project1.id}&id={workflow1b.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow1b.name

        res = await client.get(
            f"{PREFIX}/workflow/"
            f"?project_id={project1.id}&id={workflow2a.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get workflows by name
        res = await client.get(
            f"{PREFIX}/workflow/?name_contains={workflow2a.name}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == workflow2a.name

        res = await client.get(f"{PREFIX}/workflow/?name_contains=wOrKfLoW")
        assert res.status_code == 200
        assert len(res.json()) == 3

        res = await client.get(
            f"{PREFIX}/workflow/?timestamp_created_min="
            f"{quote('2000-01-01T01:01:01')}"
        )
        assert res.status_code == 422
        assert "naive" in res.json()["detail"]
        res = await client.get(
            f"{PREFIX}/workflow/?timestamp_created_max="
            f"{quote('2000-01-01T01:01:01')}"
        )
        assert res.status_code == 422
        assert "naive" in res.json()["detail"]

        res = await client.get(
            f"{PREFIX}/workflow/?timestamp_created_min=2000-01-01T01:01:01Z"
        )
        assert res.status_code == 200
        assert len(res.json()) == 4

        res = await client.get(
            f"{PREFIX}/workflow/?timestamp_created_min="
            f"{quote('2000-01-01T01:01:01+00:00')}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 4

        # same as "quote('2000-01-01T01:01:01+00:00')"
        res = await client.get(
            f"{PREFIX}/workflow/?timestamp_created_min="
            "2000-01-01T01%3A01%3A01%2B00%3A00"
        )
        assert res.status_code == 200
        assert len(res.json()) == 4

        workflow1b_timestamp_created = workflow1b.timestamp_created.isoformat()
        res = await client.get(
            f"{PREFIX}/workflow/"
            f"?timestamp_created_min={quote(workflow1b_timestamp_created)}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 3

        res = await client.get(
            f"{PREFIX}/workflow/"
            f"?timestamp_created_max={quote(workflow1b_timestamp_created)}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2


async def test_view_dataset(
    client, MockCurrentUser, project_factory, dataset_factory
):

    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:

        project1 = await project_factory(user)

        ds1a = await dataset_factory(
            project_id=project1.id,
            name="ds1a",
            type="zarr",
        )
        ds1b = await dataset_factory(
            project_id=project1.id,
            name="ds1b",
            type="image",
        )

        project2 = await project_factory(user)

        await dataset_factory(
            project_id=project2.id,
            name="ds2a",
            type="zarr",
        )

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True}
    ) as superuser:
        super_project = await project_factory(superuser)
        await dataset_factory(
            project_id=super_project.id,
            name="super-d",
            type="zarr",
        )

        # get all datasets
        res = await client.get(f"{PREFIX}/dataset/")
        assert res.status_code == 200
        assert len(res.json()) == 4

        # get datasets by user_id
        res = await client.get(f"{PREFIX}/dataset/?user_id={user.id}")
        assert res.status_code == 200
        assert len(res.json()) == 3

        # get datasets by id
        res = await client.get(f"{PREFIX}/dataset/?id={ds1a.id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == ds1a.name
        res = await client.get(f"{PREFIX}/dataset/?id=123456789")
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get datasets by project_id
        res = await client.get(f"{PREFIX}/dataset/?project_id={project1.id}")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/dataset/?project_id={project2.id}")
        assert res.status_code == 200
        assert len(res.json()) == 1

        # get datasets by name
        res = await client.get(
            f"{PREFIX}/dataset/?project_id={project1.id}&name_contains=a"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == ds1a.name
        res = await client.get(
            f"{PREFIX}/dataset/?project_id={project1.id}&name_contains=c"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get datasets by type
        res = await client.get(
            f"{PREFIX}/dataset/?type=zarr&user_id={user.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/dataset/?type=image")
        assert res.status_code == 200
        assert len(res.json()) == 1

        res = await client.get(
            f"{PREFIX}/dataset/?timestamp_created_min="
            f"{quote('2000-01-01T01:01:01')}"
        )
        assert res.status_code == 422
        assert "naive" in res.json()["detail"]
        res = await client.get(
            f"{PREFIX}/dataset/?timestamp_created_max="
            f"{quote('2000-01-01T01:01:01')}"
        )
        assert res.status_code == 422
        assert "naive" in res.json()["detail"]

        ds1b_timestamp = ds1b.timestamp_created.isoformat()
        res = await client.get(
            f"{PREFIX}/dataset/?timestamp_created_min={quote(ds1b_timestamp)}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 3

        res = await client.get(
            f"{PREFIX}/dataset/?timestamp_created_max={quote(ds1b_timestamp)}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2


async def test_view_job(
    db,
    client,
    MockCurrentUser,
    tmp_path,
    project_factory,
    workflow_factory,
    dataset_factory,
    task_factory,
    job_factory,
):
    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:

        project = await project_factory(user)

        workflow1 = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)

        task = await task_factory(name="task", source="source")
        dataset1 = await dataset_factory(project_id=project.id)
        dataset2 = await dataset_factory(project_id=project.id)

        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task.id, db=db
        )

        job1 = await job_factory(
            working_dir=f"{tmp_path.as_posix()}/aaaa1111",
            working_dir_user=f"{tmp_path.as_posix()}/aaaa2222",
            project_id=project.id,
            log="asdasd",
            input_dataset_id=dataset1.id,
            output_dataset_id=dataset2.id,
            workflow_id=workflow1.id,
            start_timestamp=datetime(2000, 1, 1, tzinfo=timezone.utc),
        )

        job2 = await job_factory(
            working_dir=f"{tmp_path.as_posix()}/bbbb1111",
            working_dir_user=f"{tmp_path.as_posix()}/bbbb2222",
            project_id=project.id,
            log="asdasd",
            input_dataset_id=dataset2.id,
            output_dataset_id=dataset1.id,
            workflow_id=workflow2.id,
            start_timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc),
            end_timestamp=datetime(2023, 11, 9, tzinfo=timezone.utc),
        )

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        # get all jobs
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 2
        assert res.json()[0]["log"] == "asdasd"
        res = await client.get(f"{PREFIX}/job/?log=false")
        assert res.status_code == 200
        assert len(res.json()) == 2
        assert res.json()[0]["log"] is None

        # get jobs by user_id
        res = await client.get(f"{PREFIX}/job/?user_id={user.id}")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/job/?user_id={user.id + 1}")
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get jobs by id
        res = await client.get(f"{PREFIX}/job/?id={job1.id}")
        assert res.status_code == 200
        assert len(res.json()) == 1

        # get jobs by project_id
        res = await client.get(f"{PREFIX}/job/?project_id={project.id}")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/job/?project_id={project.id + 123456789}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get jobs by [input/output]_dataset_id
        res = await client.get(f"{PREFIX}/job/?input_dataset_id={dataset1.id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == job1.id
        res = await client.get(
            f"{PREFIX}/job/?output_dataset_id={dataset1.id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == job2.id

        # get jobs by workflow_id
        res = await client.get(f"{PREFIX}/job/?workflow_id={workflow2.id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/job/?workflow_id=123456789")
        assert res.status_code == 200
        assert len(res.json()) == 0

        # get jobs by status
        res = await client.get(f"{PREFIX}/job/?status=failed")
        assert res.status_code == 200
        assert len(res.json()) == 0
        res = await client.get(f"{PREFIX}/job/?status=submitted")
        assert res.status_code == 200
        assert len(res.json()) == 2

        # get jobs by [start/end]_timestamp_[min/max]

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_min={quote('1999-01-01T00:00:01')}"
        )
        assert res.status_code == 422
        assert "naive" in res.json()["detail"]
        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_max={quote('1999-01-01T00:00:01')}"
        )
        assert res.status_code == 422
        assert "naive" in res.json()["detail"]

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_min="
            f"{quote('1999-01-01T00:00:01+00:00')}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_min=1999-01-01T00:00:01Z"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_max="
            f"{quote('1999-01-01T00:00:01+00:00')}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0

        res = await client.get(
            f"{PREFIX}/job/?end_timestamp_min="
            f"{quote('3000-01-01T00:00:01+00:00')}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0

        res = await client.get(
            f"{PREFIX}/job/?end_timestamp_max=3000-01-01T00:00:01Z"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1


async def test_view_single_job(
    db,
    client,
    MockCurrentUser,
    tmp_path,
    project_factory,
    workflow_factory,
    dataset_factory,
    task_factory,
    job_factory,
):
    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:

        project = await project_factory(user)

        workflow1 = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)

        task = await task_factory(name="task", source="source")
        dataset1 = await dataset_factory(project_id=project.id)
        dataset2 = await dataset_factory(project_id=project.id)

        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task.id, db=db
        )

        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=dataset1.id,
            output_dataset_id=dataset2.id,
            workflow_id=workflow1.id,
            status="submitted",
        )

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):

        res = await client.get(f"{PREFIX}/job/{job.id + 1}/")
        assert res.status_code == 404

        res = await client.get(f"{PREFIX}/job/{job.id}/")
        assert res.status_code == 200
        assert res.json()["log"] is None

        res = await client.get(f"{PREFIX}/job/{job.id}/?show_tmp_logs=true")
        assert res.status_code == 200
        assert res.json()["log"] is None

        logfile = Path(job.working_dir) / WORKFLOW_LOG_FILENAME
        assert not logfile.exists()
        with logfile.open("w") as f:
            f.write("LOG")

        res = await client.get(f"{PREFIX}/job/{job.id}/?show_tmp_logs=true")
        assert res.status_code == 200
        assert res.json()["log"] == "LOG"


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
    ORIGINAL_STATUS = JobStatusTypeV1.SUBMITTED
    NEW_STATUS = JobStatusTypeV1.FAILED

    async with MockCurrentUser() as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
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
        res = await client.get(f"/api/v1/project/{project.id}/job/{job.id}/")
        assert res.status_code == 200
        assert res.json()["status"] == ORIGINAL_STATUS
        assert res.json()["end_timestamp"] is None

        # Patch job as job owner (standard user) and fail
        res = await client.patch(
            f"{PREFIX}/job/{job.id}/",
            json={"status": NEW_STATUS},
        )
        assert res.status_code == 401

        # Patch job as superuser
        async with MockCurrentUser(user_kwargs={"is_superuser": True}):
            # Fail due to invalid payload (missing attribute "status")
            res = await registered_superuser_client.patch(
                f"{PREFIX}/job/{job.id}/",
                json={"working_dir": "/tmp"},
            )
            assert res.status_code == 422
            # Fail due to invalid payload (status not part of JobStatusType)
            res = await registered_superuser_client.patch(
                f"{PREFIX}/job/{job.id}/",
                json={"status": "something_invalid"},
            )
            assert res.status_code == 422
            # Fail due to invalid payload (status not failed)
            res = await registered_superuser_client.patch(
                f"{PREFIX}/job/{job.id}/",
                json={"status": "done"},
            )
            assert res.status_code == 422
            # Fail due to non-existing job
            res = await registered_superuser_client.patch(
                f"{PREFIX}/job/{123456789}/",
                json={"status": NEW_STATUS},
            )
            assert res.status_code == 404
            # Successfully apply patch
            res = await registered_superuser_client.patch(
                f"{PREFIX}/job/{job.id}/",
                json={"status": NEW_STATUS},
            )
            assert res.status_code == 200
            debug(res.json())
            assert res.json()["status"] == NEW_STATUS
            assert res.json()["end_timestamp"] is not None

        # Read job as job owner (standard user)
        res = await client.get(f"/api/v1/project/{project.id}/job/{job.id}/")
        assert res.status_code == 200
        assert res.json()["status"] == NEW_STATUS
        assert res.json()["end_timestamp"] is not None


@pytest.mark.parametrize("backend", backends_available)
async def test_stop_job(
    backend,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    task_factory,
    registered_superuser_client,
    db,
    tmp_path,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=backend)

    async with MockCurrentUser() as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="source")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory(project_id=project.id)
        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            workflow_id=workflow.id,
            status=JobStatusTypeV1.SUBMITTED,
        )

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):

        res = await registered_superuser_client.get(
            f"{PREFIX}/job/{job.id}/stop/",
        )

        if backend == "slurm":
            assert res.status_code == 202
            shutdown_file = tmp_path / SHUTDOWN_FILENAME
            debug(shutdown_file)
            assert shutdown_file.exists()
            res = await registered_superuser_client.get(
                f"{PREFIX}/job/{job.id + 42}/stop/",
            )
            assert res.status_code == 404
        else:
            assert res.status_code == 422


async def test_download_job_logs(
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
    async with MockCurrentUser() as user:
        prj = await project_factory(user)
        input_dataset = await dataset_factory(project_id=prj.id, name="input")
        output_dataset = await dataset_factory(
            project_id=prj.id, name="output"
        )
        workflow = await workflow_factory(project_id=prj.id)
        task = await task_factory()
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        working_dir = (tmp_path / "working_dir_for_zipping").as_posix()
        job = await job_factory(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=working_dir,
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset.id,
        )

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):

        # Write a log file in working_dir
        LOG_CONTENT = "This is a log\n"
        LOG_FILE = "log.txt"
        Path(working_dir).mkdir()
        with (Path(working_dir) / LOG_FILE).open("w") as f:
            f.write(LOG_CONTENT)

        # Test 404
        res = await client.get(f"{PREFIX}/job/{job.id + 42}/download/")
        assert res.status_code == 404

        # Test that the endpoint returns a list with the new job
        res = await client.get(f"{PREFIX}/job/{job.id}/download/")
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
