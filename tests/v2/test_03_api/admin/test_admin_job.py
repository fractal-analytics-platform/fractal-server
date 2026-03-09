from datetime import datetime
from datetime import timezone
from pathlib import Path
from urllib.parse import quote
from zipfile import ZipFile

import pytest
from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v2 import HistoryRun
from fractal_server.app.models.v2 import HistoryUnit
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.routes.aux._runner import _backend_supports_shutdown
from fractal_server.app.schemas.v2 import JobStatusType
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.runner.filenames import WORKFLOW_LOG_FILENAME

PREFIX = "/admin/v2"


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
    local_resource_profile_db,
    slurm_sudo_resource_profile_db,
):
    async with MockCurrentUser(
        is_superuser=False, profile_id=local_resource_profile_db[1].id
    ) as user1:
        project = await project_factory(user1)

        workflow1 = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)

        task = await task_factory(user_id=user1.id, name="task")
        dataset = await dataset_factory(project_id=project.id)

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
            log="log-a",
            dataset_id=dataset.id,
            workflow_id=workflow1.id,
            start_timestamp=datetime(2000, 1, 1, tzinfo=timezone.utc),
        )

        await job_factory(
            working_dir=f"{tmp_path.as_posix()}/bbbb1111",
            working_dir_user=f"{tmp_path.as_posix()}/bbbb2222",
            project_id=project.id,
            log="log-b",
            dataset_id=dataset.id,
            status=JobStatusType.DONE,
            workflow_id=workflow2.id,
            start_timestamp=datetime(2023, 1, 1, tzinfo=timezone.utc),
            end_timestamp=datetime(2023, 11, 9, tzinfo=timezone.utc),
        )

    async with MockCurrentUser(
        is_superuser=False, profile_id=slurm_sudo_resource_profile_db[1].id
    ) as user2:
        project2 = await project_factory(user2)

        workflow3 = await workflow_factory(project_id=project2.id)
        workflow4 = await workflow_factory(project_id=project2.id)

        task2 = await task_factory(user_id=user2.id, name="task2")
        dataset2 = await dataset_factory(project_id=project2.id)

        await _workflow_insert_task(
            workflow_id=workflow3.id, task_id=task2.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow4.id, task_id=task2.id, db=db
        )

        await job_factory(
            working_dir=f"{tmp_path.as_posix()}/cccc1111",
            working_dir_user=f"{tmp_path.as_posix()}/cccc2222",
            project_id=project2.id,
            log="log-c",
            dataset_id=dataset2.id,
            workflow_id=workflow3.id,
            start_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    async with MockCurrentUser(is_superuser=True):
        # get all jobs, with logs
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3
        assert res.json()["items"][0]["log"] == "log-c"

        # get all jobs, without logs
        res = await client.get(f"{PREFIX}/job/?log=false")
        assert res.status_code == 200
        assert res.json()["total_count"] == 3
        assert res.json()["items"][0]["log"] is None

        # get second page of all jobs
        res = await client.get(f"{PREFIX}/job/?page_size=1&page=2")
        assert res.status_code == 200
        assert res.json()["total_count"] == 3
        assert res.json()["page_size"] == 1
        assert res.json()["current_page"] == 2
        assert res.json()["items"][0]["log"] == "log-b"

        # get jobs by user_id
        res = await client.get(f"{PREFIX}/job/?user_id={user1.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/job/?user_id={user2.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1
        res = await client.get(f"{PREFIX}/job/?user_id={user1.id + user2.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 0

        # get jobs by id
        res = await client.get(f"{PREFIX}/job/?id={job1.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1

        # get jobs by project_id
        res = await client.get(f"{PREFIX}/job/?project_id={project.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(
            f"{PREFIX}/job/?project_id={project.id + 123456789}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 0

        # get jobs by [input/output]_dataset_id
        res = await client.get(f"{PREFIX}/job/?dataset_id={dataset.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2

        # get jobs by workflow_id
        res = await client.get(f"{PREFIX}/job/?workflow_id={workflow2.id}")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1
        res = await client.get(f"{PREFIX}/job/?workflow_id=123456789")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 0

        # get jobs by status
        res = await client.get(f"{PREFIX}/job/?status=failed")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 0
        res = await client.get(f"{PREFIX}/job/?status=submitted")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(f"{PREFIX}/job/?status=done")
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1

        # get jobs by [start/end]_timestamp_[min/max]

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_min={quote('1999-01-01T00:00:01')}"
        )
        assert res.status_code == 422  # because timezonee is None
        assert "timezone" in res.json()["detail"][0]["msg"]

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_min="
            f"{quote('1999-01-01T00:00:01+00:00')}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_min=1999-01-01T00:00:01Z"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 3

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_max={quote('1999-01-01T00:00:01')}"
        )
        assert res.status_code == 422  # because timezonee is None
        assert "timezone" in res.json()["detail"][0]["msg"]

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_max="
            f"{quote('1999-01-01T00:00:01+00:00')}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 0

        res = await client.get(
            f"{PREFIX}/job/?end_timestamp_min="
            f"{quote('3000-01-01T00:00:01+00:00')}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 0

        res = await client.get(
            f"{PREFIX}/job/?end_timestamp_max=3000-01-01T00:00:01Z"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1

        res = await client.get(
            f"{PREFIX}/job/?resource_id={local_resource_profile_db[1].id}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 2
        res = await client.get(
            f"{PREFIX}/job/?resource_id={slurm_sudo_resource_profile_db[1].id}"
        )
        assert res.status_code == 200
        assert len(res.json()["items"]) == 1


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
    async with MockCurrentUser(is_superuser=False) as user:
        project = await project_factory(user)

        workflow1 = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)

        task = await task_factory(user_id=user.id, name="task")
        dataset = await dataset_factory(project_id=project.id)

        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task.id, db=db
        )

        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow1.id,
            status="submitted",
        )

    async with MockCurrentUser(is_superuser=True):
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
    ORIGINAL_STATUS = JobStatusType.SUBMITTED
    NEW_STATUS = JobStatusType.FAILED

    async with MockCurrentUser() as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(user_id=user.id, name="task")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory(project_id=project.id)
        job = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            status=ORIGINAL_STATUS,
        )
        job_done = await job_factory(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            status=JobStatusType.DONE,
        )

        hr = HistoryRun(
            dataset_id=dataset.id,
            job_id=job.id,
            workflowtask_dump={},
            task_group_dump={},
            num_available_images=0,
            status=ORIGINAL_STATUS,
        )
        db.add(hr)
        await db.commit()
        await db.refresh(hr)
        db.add_all(
            [
                HistoryUnit(
                    history_run_id=hr.id,
                    logfile=f"logfile_{i}",
                    status=ORIGINAL_STATUS,
                )
                for i in range(3)
            ]
        )
        await db.commit()
        db.expunge_all()

        # Read job as job owner (standard user)
        res = await client.get(f"/api/v2/project/{project.id}/job/{job.id}/")
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
        async with MockCurrentUser(is_superuser=True):
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
            # Fail because job is not in submitted state
            res = await registered_superuser_client.patch(
                f"{PREFIX}/job/{job_done.id}/",
                json={"status": NEW_STATUS},
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
            assert (
                "This job was manually marked as 'failed' by an admin"
                in res.json()["log"]
            )

            res = await db.execute(
                select(HistoryRun)
                .where(HistoryRun.job_id == job.id)
                .order_by(HistoryRun.timestamp_started.desc())
            )
            history_run = res.scalar_one_or_none()
            assert history_run.status == NEW_STATUS
            res = await db.execute(
                select(HistoryUnit).where(
                    HistoryUnit.history_run_id == history_run.id
                )
            )
            for history_units in res.scalars().all():
                assert history_units.status == NEW_STATUS

        # Read job as job owner (standard user)
        res = await client.get(f"/api/v2/project/{project.id}/job/{job.id}/")
        assert res.status_code == 200
        assert res.json()["status"] == NEW_STATUS
        assert res.json()["end_timestamp"] is not None


async def test_stop_job_local(
    MockCurrentUser,
    client,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=ResourceType.LOCAL)
    assert not _backend_supports_shutdown(backend=ResourceType.LOCAL)
    async with MockCurrentUser(is_superuser=True):
        res = await client.get(f"{PREFIX}/job/123/stop/")
        assert res.status_code == 422


@pytest.mark.parametrize(
    "backend", [ResourceType.SLURM_SUDO, ResourceType.SLURM_SSH]
)
async def test_stop_job_slurm(
    backend,
    MockCurrentUser,
    client,
    db,
    project_factory,
    dataset_factory,
    workflow_factory,
    tmp_path,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=backend)

    async with MockCurrentUser(is_superuser=True) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        dataset = await dataset_factory(project_id=project.id)
        job = JobV2(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            status=JobStatusType.SUBMITTED,
            user_email="fake@example.org",
            dataset_dump={},
            workflow_dump={},
            project_dump={},
            first_task_index=0,
            last_task_index=0,
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)

        # Stop missing job
        res = await client.get(f"{PREFIX}/job/{job.id + 42}/stop/")
        assert res.status_code == 404

        # Stop actual job
        res = await client.get(f"{PREFIX}/job/{job.id}/stop/")
        assert res.status_code == 202
        shutdown_file = tmp_path / SHUTDOWN_FILENAME
        debug(shutdown_file)
        assert shutdown_file.exists()


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
        dataset = await dataset_factory(project_id=prj.id, name="dataset")
        workflow = await workflow_factory(project_id=prj.id)
        task = await task_factory(user_id=user.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        working_dir = (tmp_path / "working_dir_for_zipping").as_posix()
        job = await job_factory(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=working_dir,
            dataset_id=dataset.id,
        )

    async with MockCurrentUser(is_superuser=True):
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
        assert res.headers.get("content-type") == "application/x-zip-compressed"

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
