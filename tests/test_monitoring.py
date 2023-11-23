from datetime import datetime

from fractal_server.app.models import JobStatusType

PREFIX = "/monitoring"


async def test_unauthorized_to_monitor(client, MockCurrentUser):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ):
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/workflow/")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/dataset/")
        assert res.status_code == 403
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 403

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/workflow/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/dataset/")
        assert res.status_code == 200
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200


async def test_monitor_project(client, MockCurrentUser, project_factory):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ) as superuser:
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert res.json() == []
        await project_factory(superuser)

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:
        project1 = await project_factory(user)
        prj1_id = project1.id
        await project_factory(user)
        user_id = user.id

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        res = await client.get(f"{PREFIX}/project/")
        assert res.status_code == 200
        assert len(res.json()) == 3
        res = await client.get(f"{PREFIX}/project/?id={prj1_id}")
        assert res.status_code == 200
        assert len(res.json()) == 1
        res = await client.get(f"{PREFIX}/project/?user_id={user_id}")
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/project/?user_id={user_id}&id={prj1_id}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1


async def test_monitor_workflow(
    client, MockCurrentUser, project_factory, workflow_factory
):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:

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
        persist=True, user_kwargs={"is_superuser": True}
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


async def test_monitor_dataset(
    client, MockCurrentUser, project_factory, dataset_factory
):

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:

        project1 = await project_factory(user)

        ds1a = await dataset_factory(
            project_id=project1.id,
            name="ds1a",
            type="zarr",
        )
        await dataset_factory(
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
        persist=True, user_kwargs={"is_superuser": True}
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
            f"{PREFIX}/dataset/" f"?project_id={project1.id}&name_contains=a"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
        assert res.json()[0]["name"] == ds1a.name
        res = await client.get(
            f"{PREFIX}/dataset/" f"?project_id={project1.id}&name_contains=c"
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


async def test_monitor_job(
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
    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": False}
    ) as user:

        project = await project_factory(user)

        workflow1 = await workflow_factory(project_id=project.id)
        workflow2 = await workflow_factory(project_id=project.id)

        task = await task_factory(name="task", source="source")
        dataset1 = await dataset_factory(project_id=project.id)
        dataset2 = await dataset_factory(project_id=project.id)

        await workflow1.insert_task(task_id=task.id, db=db)
        await workflow2.insert_task(task_id=task.id, db=db)

        job1 = await job_factory(
            working_dir=f"{tmp_path.as_posix()}/aaaa1111",
            working_dir_user=f"{tmp_path.as_posix()}/aaaa2222",
            project_id=project.id,
            input_dataset_id=dataset1.id,
            output_dataset_id=dataset2.id,
            workflow_id=workflow1.id,
            start_timestamp=datetime(2000, 1, 1),
        )

        job2 = await job_factory(
            working_dir=f"{tmp_path.as_posix()}/bbbb1111",
            working_dir_user=f"{tmp_path.as_posix()}/bbbb2222",
            project_id=project.id,
            input_dataset_id=dataset2.id,
            output_dataset_id=dataset1.id,
            workflow_id=workflow2.id,
            start_timestamp=datetime(2023, 1, 1),
            end_timestamp=datetime(2023, 11, 9),
        )

    async with MockCurrentUser(
        persist=True, user_kwargs={"is_superuser": True}
    ):
        # get all jobs
        res = await client.get(f"{PREFIX}/job/")
        assert res.status_code == 200
        assert len(res.json()) == 2

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
        res = await client.get(f"{PREFIX}/job/?status={JobStatusType.FAILED}")
        assert res.status_code == 200
        assert len(res.json()) == 0
        res = await client.get(
            f"{PREFIX}/job/?status={JobStatusType.SUBMITTED}"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2

        # get jobs by [start/end]_timestamp_[min/max]

        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_min=1999-01-01T00:00:01"
        )
        assert res.status_code == 200
        assert len(res.json()) == 2
        res = await client.get(
            f"{PREFIX}/job/?start_timestamp_max=1999-01-01T00:00:01"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0
        res = await client.get(
            f"{PREFIX}/job/?end_timestamp_min=3000-01-01T00:00:01"
        )
        assert res.status_code == 200
        assert len(res.json()) == 0
        res = await client.get(
            f"{PREFIX}/job/?end_timestamp_max=3000-01-01T00:00:01"
        )
        assert res.status_code == 200
        assert len(res.json()) == 1
