from datetime import datetime
from datetime import timezone

import pytest
from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.schemas.v2 import JobStatusTypeV2

PREFIX = "/api/v2"


async def test_post_and_get_project(client, db, MockCurrentUser):

    PAYLOAD = dict(name="project_v2")

    # unauthenticated
    res = await client.post(f"{PREFIX}/project/", json=PAYLOAD)
    assert res.status_code == 401
    res = await client.get(f"{PREFIX}/project/")
    assert res.status_code == 401

    # authenticated
    async with MockCurrentUser(user_kwargs=dict(id=1)) as userA:
        res = await client.post(
            f"{PREFIX}/project/", json=dict(name="project")
        )
        assert res.status_code == 201
        assert len(userA.project_list) == 0
        assert len(userA.project_list_v2) == 1
        other_project = res.json()

    async with MockCurrentUser(user_kwargs=dict(id=2)) as userB:

        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert res.json() == userB.project_list_v2 == []

        res = await client.post(
            f"{PREFIX}/project/", json=dict(name="project")
        )
        assert res.status_code == 201
        assert len(userB.project_list) == 0
        assert len(userB.project_list_v2) == 1

        # a user can't create two projectsV2 with the same name
        res = await client.post(
            f"{PREFIX}/project/", json=dict(name="project")
        )
        assert res.status_code == 422
        assert len(userB.project_list_v2) == 1

        # create two V1 Projects
        for i in range(2):
            res = await client.post(
                "/api/v1/project/", json=dict(name=f"project_{i}_v1")
            )
        assert len(userB.project_list) == 2
        assert len(userB.project_list_v2) == 1

        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == userB.project_list_v2[0].id

        project_id = res.json()[0]["id"]
        res = await client.get(f"{PREFIX}/project/{project_id}/")
        assert res.status_code == 200
        assert res.json()["id"] == userB.project_list_v2[0].id
        assert (
            datetime.fromisoformat(res.json()["timestamp_created"]).tzinfo
            == timezone.utc
        )

        # fail on non existent project
        res = await client.get(f"{PREFIX}/project/123456/")
        assert res.status_code == 404

        # fail on other owner's project
        res = await client.get(f"{PREFIX}/project/{other_project['id']}/")
        assert res.status_code == 403


async def test_post_project(app, client, MockCurrentUser, db):
    payload = dict(name="new project")

    # Fail for anonymous user
    res = await client.post(f"{PREFIX}/project/", json=payload)
    data = res.json()
    assert res.status_code == 401

    async with MockCurrentUser():
        res = await client.post(f"{PREFIX}/project/", json=payload)
        data = res.json()
        assert res.status_code == 201
        debug(data)
        assert data["name"] == payload["name"]

        # Payload without `name`
        empty_payload = {}
        res = await client.post(f"{PREFIX}/project/", json=empty_payload)
        debug(res.json())
        assert res.status_code == 422


async def test_post_project_name_constraint(app, client, MockCurrentUser, db):
    payload = dict(name="new project")
    res = await client.post(f"{PREFIX}/project/", json=payload)
    assert res.status_code == 401

    async with MockCurrentUser():
        # Create a first project named "new project"
        res = await client.post(f"{PREFIX}/project/", json=payload)
        assert res.status_code == 201

        # Create a second project named "new project", and check that this
        # fails with 422_UNPROCESSABLE_ENTITY
        res = await client.post(f"{PREFIX}/project/", json=payload)
        assert res.status_code == 422


async def test_patch_project_name_constraint(app, client, MockCurrentUser, db):
    async with MockCurrentUser():
        # Create a first project named "name1"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name1"))
        assert res.status_code == 201

        # Create a second project named "name2"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name2"))
        assert res.status_code == 201
        prj2 = res.json()

        # Fail in editing the name of prj2 to "name1"
        res = await client.patch(
            f"{PREFIX}/project/{prj2['id']}/", json=dict(name="name1")
        )
        assert res.status_code == 422
        assert res.json()["detail"] == "Project name (name1) already in use"

    async with MockCurrentUser():
        # Using another user, create a project named "name3"
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name3"))
        assert res.status_code == 201
        prj3 = res.json()
        # Edit the name of prj3 to "name1" without errors
        res = await client.patch(
            f"{PREFIX}/project/{prj3['id']}/", json=dict(name="name1")
        )
        debug(res.json())
        assert res.status_code == 200


@pytest.mark.parametrize("new_name", (None, "new name"))
@pytest.mark.parametrize("new_read_only", (None, True, False))
async def test_patch_project(
    new_name,
    new_read_only,
    app,
    client,
    MockCurrentUser,
    db,
):
    """
    Test that the project can be patched correctly, with any possible
    combination of set/unset attributes.
    """
    async with MockCurrentUser():
        # Create project
        payload = dict(
            name="old name",
            read_only=True,
        )
        res = await client.post(f"{PREFIX}/project/", json=payload)
        old_project = res.json()
        project_id = old_project["id"]
        assert res.status_code == 201

        # Patch project
        payload = {}
        if new_name:
            payload["name"] = new_name
        if new_read_only:
            payload["read_only"] = new_read_only
        debug(payload)
        res = await client.patch(
            f"{PREFIX}/project/{project_id}/", json=payload
        )
        new_project = res.json()
        debug(new_project)
        assert res.status_code == 200
        for key, value in new_project.items():
            if key in payload.keys():
                assert value == payload[key]
            else:
                assert value == old_project[key]


async def test_delete_project(
    client,
    MockCurrentUser,
    db,
    tmp_path,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser():
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        assert len(data) == 0

        # Create a project
        res = await client.post(f"{PREFIX}/project/", json=dict(name="name"))
        p = res.json()

        # Verify that the project was created
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        debug(data)
        assert res.status_code == 200
        assert len(data) == 1
        project_id = res.json()[0]["id"]

        # Add a dataset to the project
        dataset = await dataset_factory_v2(project_id=project_id)
        dataset_id = dataset.id

        # Add a workflow to the project
        wf = await workflow_factory_v2(project_id=p["id"])
        t = await task_factory_v2()
        await _workflow_insert_task(
            workflow_id=wf.id, task_id=t.id, is_v2=True, db=db
        )

        # Add a job to the project

        await job_factory_v2(
            project_id=p["id"],
            workflow_id=wf.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
            dataset_id=dataset_id,
            status=JobStatusTypeV2.DONE,
        )

        # Check that a project-related job exists - via query
        stm = select(JobV2).where(JobV2.project_id == p["id"])
        res = (await db.execute(stm)).scalars().all()
        assert len(res) == 1
        job = res[0]
        assert job.project_id == p["id"]
        assert job.dataset_id == dataset_id
        assert job.workflow_id == wf.id

        # Delete the project
        res = await client.delete(f"{PREFIX}/project/{p['id']}/")
        assert res.status_code == 204

        # Check that the project was deleted
        res = await client.get(f"{PREFIX}/project/")
        data = res.json()
        assert len(data) == 0

        # Check that project-related datasets were deleted
        stm = select(DatasetV2).join(ProjectV2).where(ProjectV2.id == p["id"])
        res = await db.execute(stm)
        datasets = list(res)
        debug(datasets)
        assert len(datasets) == 0

        # Check that project-related workflows were deleted
        stm = select(WorkflowV2).join(ProjectV2).where(ProjectV2.id == p["id"])
        res = await db.execute(stm)
        workflows = list(res)
        debug(workflows)
        assert len(workflows) == 0

        # Assert that total number of jobs is still 1, but without project_id
        await db.refresh(job)
        assert job.project_id is None
        assert job.dataset_id is None
        assert job.workflow_id is None


async def test_delete_project_ongoing_jobs(
    client,
    MockCurrentUser,
    db,
    tmp_path,
    project_factory_v2,
    job_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
):
    async with MockCurrentUser() as user:

        async def get_project_id_linked_to_job(status: JobStatusTypeV2) -> int:
            p = await project_factory_v2(user)
            d = await dataset_factory_v2(project_id=p.id)
            w = await workflow_factory_v2(project_id=p.id)
            t = await task_factory_v2(
                name=f"task_{status}", source=f"source_{status}"
            )
            await _workflow_insert_task(
                workflow_id=w.id, task_id=t.id, is_v2=True, db=db
            )
            await job_factory_v2(
                project_id=p.id,
                workflow_id=w.id,
                dataset_id=d.id,
                working_dir=(tmp_path / "some_working_dir").as_posix(),
                status=status,
            )
            return p.id

        prj_done = await get_project_id_linked_to_job(JobStatusTypeV2.DONE)
        prj_failed = await get_project_id_linked_to_job(JobStatusTypeV2.FAILED)
        prj_submitted = await get_project_id_linked_to_job(
            JobStatusTypeV2.SUBMITTED
        )

        res = await client.delete(f"api/v2/project/{prj_done}/")
        assert res.status_code == 204
        res = await client.delete(f"api/v2/project/{prj_failed}/")
        assert res.status_code == 204
        res = await client.delete(f"api/v2/project/{prj_submitted}/")
        assert res.status_code == 422
