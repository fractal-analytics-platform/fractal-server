from devtools import debug

PREFIX = "/api/v1"


async def test_project_apply_failures(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
):
    async with MockCurrentUser(persist=True) as user:
        project1 = await project_factory(user)
        project2 = await project_factory(user)
        input_dataset = await dataset_factory(project1, name="input")
        output_dataset = await dataset_factory(project1, name="output")
        output_dataset_read_only = await dataset_factory(
            project1, name="output", read_only=True
        )
        output_dataset_wrong_type = await dataset_factory(
            project1, name="output", type="invalid_type"
        )
        output_dataset_two_resources = await dataset_factory(
            project1, name="output"
        )

        await resource_factory(input_dataset)
        await resource_factory(output_dataset)
        await resource_factory(output_dataset_read_only)
        await resource_factory(output_dataset_wrong_type)
        await resource_factory(output_dataset_two_resources)
        await resource_factory(output_dataset_two_resources)

        workflow1 = await workflow_factory(project_id=project1.id)
        workflow2 = await workflow_factory(project_id=project1.id)
        workflow3 = await workflow_factory(project_id=project2.id)

        task = await task_factory()
        await workflow1.insert_task(task.id, db=db)

        # Not existing workflow
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/123/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # Workflow with wrong project_id
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow3.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422

        # Not existing output dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}&output_dataset_id=123",
            json={},
        )
        debug(res.json())
        assert res.status_code == 404

        # Missing output_dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422

        # Read-only output_dataset
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_read_only.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "read_only" in res.json()["detail"]

        # output_dataset with wrong type
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_wrong_type.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "Incompatible types" in res.json()["detail"]

        # output_dataset with two resources
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow1.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset_two_resources.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "must have a single resource" in res.json()["detail"]

        # Workflow without tasks
        res = await client.post(
            f"{PREFIX}/project/{project1.id}/workflow/{workflow2.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "empty task list" in res.json()["detail"]


async def test_project_apply_missing_user_attributes(
    db,
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    resource_factory,
    workflow_factory,
    task_factory,
    override_settings_factory,
):
    """
    When using the slurm backend, user.slurm_user and user.cache_dir become
    required attributes. If they are missing, the apply endpoint fails with a
    422 error.
    """

    override_settings_factory(FRACTAL_RUNNER_BACKEND="slurm")

    async with MockCurrentUser(persist=True) as user:

        # Make sure that user.cache_dir was not set
        debug(user)
        assert user.cache_dir is None

        # Create project, datasets, workflow, task, workflowtask
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project, name="input", type="zarr"
        )
        output_dataset = await dataset_factory(project, name="output")
        for dataset_id in [input_dataset.id, output_dataset.id]:
            res = await client.post(
                f"{PREFIX}/project/{project.id}/"
                f"dataset/{dataset_id}/resource/",
                json=dict(path="/some/absolute/path"),
            )
            assert res.status_code == 201
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(input_type="zarr")
        await workflow.insert_task(task.id, db=db)

        # Call apply endpoint
        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "user.cache_dir=None" in res.json()["detail"]

        user.cache_dir = "/tmp"
        user.slurm_user = None
        await db.merge(user)
        await db.commit()

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )
        debug(res.json())
        assert res.status_code == 422
        assert "user.slurm_user=None" in res.json()["detail"]


async def test_project_apply_missing_resources(
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    task_factory,
    db,
    client,
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        input_dataset = await dataset_factory(
            project, name="input", type="zarr"
        )
        output_dataset = await dataset_factory(project, name="output")
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory()
        await workflow.insert_task(task.id, db=db)

        debug(input_dataset)
        debug(output_dataset)

        res = await client.post(
            f"{PREFIX}/project/{project.id}/workflow/{workflow.id}/apply/"
            f"?input_dataset_id={input_dataset.id}"
            f"&output_dataset_id={output_dataset.id}",
            json={},
        )

        debug(res.json())
        assert res.status_code == 422
        assert "empty resource_list" in res.json()["detail"]
