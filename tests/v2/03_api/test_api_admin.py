from datetime import datetime
from datetime import timezone
from pathlib import Path
from urllib.parse import quote
from zipfile import ZipFile

import pytest
from devtools import debug

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserGroup
from fractal_server.app.models.v2 import CollectionStateV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.filenames import SHUTDOWN_FILENAME
from fractal_server.app.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.app.runner.v1 import _backends
from fractal_server.app.schemas.v2 import JobStatusTypeV2

backends_available = list(_backends.keys())

PREFIX = "/admin/v2"


async def test_unauthorized_to_admin(client, MockCurrentUser):

    async with MockCurrentUser(user_kwargs={"is_superuser": False}):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 401

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200


async def test_view_project(client, MockCurrentUser, project_factory_v2):

    async with MockCurrentUser(
        user_kwargs={"is_superuser": True}
    ) as superuser:
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert res.json() == []
        await project_factory_v2(superuser)

    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:
        project = await project_factory_v2(user)
        prj_id = project.id
        await project_factory_v2(user)
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

        res = await client.get(f"{PREFIX}/project/?id=9999999")
        assert res.status_code == 200
        assert res.json() == []
        res = await client.get(f"{PREFIX}/project/?user_id=9999999")
        assert res.status_code == 200
        assert res.json() == []


async def test_view_job(
    db,
    client,
    MockCurrentUser,
    tmp_path,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    job_factory_v2,
):
    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:

        project = await project_factory_v2(user)

        workflow1 = await workflow_factory_v2(project_id=project.id)
        workflow2 = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(
            user_id=user.id, name="task", source="source"
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task.id, db=db
        )

        job1 = await job_factory_v2(
            working_dir=f"{tmp_path.as_posix()}/aaaa1111",
            working_dir_user=f"{tmp_path.as_posix()}/aaaa2222",
            project_id=project.id,
            log="asdasd",
            dataset_id=dataset.id,
            workflow_id=workflow1.id,
            start_timestamp=datetime(2000, 1, 1, tzinfo=timezone.utc),
        )

        await job_factory_v2(
            working_dir=f"{tmp_path.as_posix()}/bbbb1111",
            working_dir_user=f"{tmp_path.as_posix()}/bbbb2222",
            project_id=project.id,
            log="asdasd",
            dataset_id=dataset.id,
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
        res = await client.get(f"{PREFIX}/job/?dataset_id={dataset.id}")
        assert res.status_code == 200
        assert len(res.json()) == 2

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
        assert res.status_code == 422  # because timezonee is None
        assert "timezone" in res.json()["detail"]

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
            f"{PREFIX}/job/?start_timestamp_max={quote('1999-01-01T00:00:01')}"
        )
        assert res.status_code == 422  # because timezonee is None
        assert "timezone" in res.json()["detail"]

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
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    task_factory_v2,
    job_factory_v2,
):
    async with MockCurrentUser(user_kwargs={"is_superuser": False}) as user:

        project = await project_factory_v2(user)

        workflow1 = await workflow_factory_v2(project_id=project.id)
        workflow2 = await workflow_factory_v2(project_id=project.id)

        task = await task_factory_v2(
            user_id=user.id, name="task", source="source"
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task.id, db=db
        )

        job = await job_factory_v2(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=dataset.id,
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
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    task_factory_v2,
    client,
    registered_superuser_client,
    db,
    tmp_path,
):
    ORIGINAL_STATUS = JobStatusTypeV2.SUBMITTED
    NEW_STATUS = JobStatusTypeV2.FAILED

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(
            user_id=user.id, name="task", source="source"
        )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)
        job = await job_factory_v2(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            status=ORIGINAL_STATUS,
        )
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
        res = await client.get(f"/api/v2/project/{project.id}/job/{job.id}/")
        assert res.status_code == 200
        assert res.json()["status"] == NEW_STATUS
        assert res.json()["end_timestamp"] is not None


@pytest.mark.parametrize("backend", backends_available)
async def test_stop_job(
    backend,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    task_factory_v2,
    registered_superuser_client,
    db,
    tmp_path,
    override_settings_factory,
):
    override_settings_factory(FRACTAL_RUNNER_BACKEND=backend)

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(
            user_id=user.id, name="task", source="source"
        )
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)
        job = await job_factory_v2(
            working_dir=tmp_path.as_posix(),
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            status=JobStatusTypeV2.SUBMITTED,
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
    project_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    task_factory_v2,
    db,
    tmp_path,
):
    async with MockCurrentUser() as user:
        prj = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=prj.id, name="dataset")
        workflow = await workflow_factory_v2(project_id=prj.id)
        task = await task_factory_v2(user_id=user.id)
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        working_dir = (tmp_path / "working_dir_for_zipping").as_posix()
        job = await job_factory_v2(
            project_id=prj.id,
            workflow_id=workflow.id,
            working_dir=working_dir,
            dataset_id=dataset.id,
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


async def test_task_query(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser(user_kwargs={"is_superuser": True}) as user:

        project = await project_factory_v2(user)

        workflow1 = await workflow_factory_v2(project_id=project.id)
        workflow2 = await workflow_factory_v2(project_id=project.id)

        task1 = await task_factory_v2(
            user_id=user.id, name="Foo", source="xxx"
        )
        task2 = await task_factory_v2(
            user_id=user.id, name="abcdef", source="yyy"
        )
        task3 = await task_factory_v2(user_id=user.id, index=3)

        # task1 to workflow 1 and 2
        await _workflow_insert_task(
            workflow_id=workflow1.id, task_id=task1.id, db=db
        )
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task1.id, db=db
        )
        # task2 to workflow2
        await _workflow_insert_task(
            workflow_id=workflow2.id, task_id=task2.id, db=db
        )
        # task3 is orphan

        # Query ALL Tasks

        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 200
        assert len(res.json()) == 3
        debug(res.json())

        # Query by ID

        res = await client.get(f"{PREFIX}/task/?id={task1.id}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"].items() <= task1.model_dump().items()
        assert len(res.json()[0]["relationships"]) == 2
        _common_args = dict(
            project_id=project.id,
            project_name=project.name,
            project_users=[dict(id=user.id, email=user.email)],
        )
        assert res.json()[0]["relationships"][0] == dict(
            workflow_id=workflow1.id,
            workflow_name=workflow1.name,
            **_common_args,
        )
        assert res.json()[0]["relationships"][1] == dict(
            workflow_id=workflow2.id,
            workflow_name=workflow2.name,
            **_common_args,
        )

        res = await client.get(f"{PREFIX}/task/?id={task2.id}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task2.id
        assert len(res.json()[0]["relationships"]) == 1

        res = await client.get(f"{PREFIX}/task/?id={task3.id}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task3.id
        assert len(res.json()[0]["relationships"]) == 0

        res = await client.get(f"{PREFIX}/task/?id=1000")
        assert len(res.json()) == 0

        # Query by SOURCE

        res = await client.get(f"{PREFIX}/task/?source={task1.source}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task1.id
        assert len(res.json()[0]["relationships"]) == 2

        res = await client.get(f"{PREFIX}/task/?source={task1.source[0]}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task1.id
        assert len(res.json()[0]["relationships"]) == 2

        res = await client.get(f"{PREFIX}/task/?source={task2.source}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task2.id
        assert len(res.json()[0]["relationships"]) == 1

        res = await client.get(f"{PREFIX}/task/?source={task3.source}")
        assert len(res.json()) == 1
        assert res.json()[0]["task"]["id"] == task3.id
        assert len(res.json()[0]["relationships"]) == 0

        res = await client.get(f"{PREFIX}/task/?source=foo")
        assert len(res.json()) == 0

        # Query by VERSION

        res = await client.get(f"{PREFIX}/task/?version=0")  # task 1 + 2
        assert len(res.json()) == 2

        res = await client.get(f"{PREFIX}/task/?version=3")  # task 3
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?version=1.2")
        assert len(res.json()) == 0

        # Query by NAME

        res = await client.get(f"{PREFIX}/task/?name={task1.name}")
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?name={task2.name}")
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?name={task3.name}")
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task/?name=nonamelikethis")
        assert len(res.json()) == 0

        res = await client.get(f"{PREFIX}/task/?name=f")  # task 1 + 2
        assert len(res.json()) == 2

        res = await client.get(f"{PREFIX}/task/?name=F")  # task 1 + 2
        assert len(res.json()) == 2

        # --------------------------
        # Relationships after deleting the Project

        res = await client.delete(f"api/v2/project/{project.id}/")
        assert res.status_code == 204

        # Query by ID

        for t in [task1, task2, task3]:
            res = await client.get(f"{PREFIX}/task/?id={t.id}")
            assert len(res.json()) == 1
            assert res.json()[0]["task"].items() <= t.model_dump().items()
            assert res.json()[0]["task"]["id"] == t.id
            assert len(res.json()[0]["relationships"]) == 0

        # Query by SOURCE
        for t in [task1, task2, task3]:
            res = await client.get(f"{PREFIX}/task/?source={t.source}")
            assert len(res.json()) == 1
            assert res.json()[0]["task"]["id"] == t.id
            assert len(res.json()[0]["relationships"]) == 0

        # --------------------------
        # Too many Tasks

        # We need 'db.close' to avoid: "<sqlalchemy.exc.SAWarning: Identity map
        # already had an identity for (<class 'fractal_server.app.models.v2.*'>
        # ,(1,), None), replacing it with newly flushed object." where * is in
        # [project.ProjectV2, workflow.WorkflowV2, workflowtask.WorkflowTaskV2]
        await db.close()

        new_project = await project_factory_v2(user)
        new_workflow = await workflow_factory_v2(project_id=new_project.id)

        for i in range(30):
            task = await task_factory_v2(
                user_id=user.id, name=f"n{i}", source=f"s{i}"
            )
            await _workflow_insert_task(
                workflow_id=new_workflow.id, task_id=task.id, db=db
            )
        res = await client.get(f"{PREFIX}/task/")
        assert res.status_code == 422
        assert "Please add more query filters" in res.json()["detail"]


async def test_task_group_admin(
    db,
    client,
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser() as user1:
        task1 = await task_factory_v2(user_id=user1.id, source="source1")
        res = await client.get(f"/api/v2/task-group/{task1.taskgroupv2_id}/")
        task_group_1 = res.json()
        task2 = await task_factory_v2(
            user_id=user1.id, source="source2", active=False
        )
        # make task_group_2 private
        task_group_2 = await db.get(TaskGroupV2, task2.taskgroupv2_id)
        task_group_2.user_group_id = None
        db.add(task_group_2)
        await db.commit()
        res = await client.get(f"/api/v2/task-group/{task2.taskgroupv2_id}/")
        task_group_2 = res.json()
        debug(task_group_2)

    async with MockCurrentUser() as user2:
        task3 = await task_factory_v2(user_id=user2.id, source="source3")
        res = await client.get(f"/api/v2/task-group/{task3.taskgroupv2_id}/")
        task_group_3 = res.json()

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):

        # GET /{id}/
        for task_group in [task_group_1, task_group_2, task_group_3]:
            res = await client.get(f"{PREFIX}/task-group/{task_group['id']}/")
            assert res.status_code == 200
        res = await client.get(f"{PREFIX}/task-group/9999/")
        assert res.status_code == 404

        # GET /
        res = await client.get(f"{PREFIX}/task-group/")
        assert res.status_code == 200
        assert len(res.json()) == 3

        res = await client.get(f"{PREFIX}/task-group/?user_id={user1.id}")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/task-group/?user_id={user2.id}")
        assert res.status_code == 200
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task-group/?active=true")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/task-group/?user_id={user1.id}&active=true"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(
            f"{PREFIX}/task-group/?user_id={user1.id}&active=false"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1

        res = await client.get(f"{PREFIX}/task-group/?private=true")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/task-group/?private=false")
        assert res.status_code == 200
        assert len(res.json()) == 2

        res = await client.get(f"{PREFIX}/task-group/?user_group_id=1")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/task-group/?user_group_id=1&private=true"
        )
        assert res.status_code == 422

        # PATCH /{id}/
        res = await client.patch(
            f"{PREFIX}/task-group/9999/", json=dict(user_group_id=None)
        )
        assert res.status_code == 404

        user_group = UserGroup(name="foo")
        db.add(user_group)
        await db.commit()
        await db.refresh(user_group)
        res = await client.patch(
            f"{PREFIX}/task-group/{task_group_1['id']}/",
            json=dict(user_group_id=user_group.id),
        )
        assert res.status_code == 403
        res = await client.patch(
            f"{PREFIX}/task-group/{task_group_1['id']}/",
            json=dict(user_group_id=None, active=False),
        )
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/task-group/?private=true")
        assert len(res.json()) == 2
        res = await client.get(f"{PREFIX}/task-group/?active=true")
        assert len(res.json()) == 1

        res = await client.patch(
            f"{PREFIX}/task-group/{task_group_1['id']}/",
            json=dict(active=None),
        )
        assert res.status_code == 422

    # DELETE
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id, source="source")
        await workflowtask_factory_v2(workflow_id=workflow.id, task_id=task.id)

    state = CollectionStateV2(taskgroupv2_id=task_group_1["id"])
    db.add(state)
    await db.commit()
    await db.refresh(state)
    assert state.taskgroupv2_id == task_group_1["id"]

    async with MockCurrentUser(user_kwargs={"is_superuser": True}):
        res = await client.delete(f"{PREFIX}/task-group/{task_group_1['id']}/")
        assert res.status_code == 204
        res = await client.delete(f"{PREFIX}/task-group/{task_group_2['id']}/")
        assert res.status_code == 204
        res = await client.delete(f"{PREFIX}/task-group/{task_group_3['id']}/")
        assert res.status_code == 204
        res = await client.delete(f"{PREFIX}/task-group/9999/")
        assert res.status_code == 404
        res = await client.delete(
            f"{PREFIX}/task-group/{task.taskgroupv2_id}/"
        )
        assert res.status_code == 422

    await db.refresh(state)
    assert state.taskgroupv2_id is None
