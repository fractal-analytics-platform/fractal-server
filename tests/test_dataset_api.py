from devtools import debug

PREFIX = "api/v1"


async def test_get_resource(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project)
        res = await client.get(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/resource/"
        )
        debug(res)
        assert res.status_code == 200


async def test_error_in_update(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/resource/",
            json=dict(path="/tmp/xyz"),
        )
        debug(res)
        assert res.status_code == 201
        resource = res.json()

        other_dataset = await dataset_factory(project)

        res = await client.patch(
            f"{PREFIX}/project/{project.id}/dataset/{other_dataset.id}/"
            f"resource/{resource['id']}",
            json=dict(path="/tmp/abc"),
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            f"Resource {resource['id']} is not part "
            f"of dataset {other_dataset.id}"
        )


async def test_delete_resource(
    db, client, MockCurrentUser, project_factory, dataset_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        dataset = await dataset_factory(project)
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/resource/",
            json=dict(path="/tmp/xyz"),
        )
        debug(res)
        assert res.status_code == 201
        resource = res.json()

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            f"resource/{resource['id']+1}"
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Resource does not exist or does not belong to project"
        )

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            f"resource/{resource['id']}"
        )
        assert res.status_code == 204

        other_project = await project_factory(user)
        other_dataset = await dataset_factory(other_project)
        res = await client.post(
            f"{PREFIX}/project/{other_project.id}/"
            f"dataset/{other_dataset.id}/resource/",
            json=dict(path="/tmp/xyz"),
        )
        assert res.status_code == 201
        other_resource = res.json()

        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
            f"resource/{other_resource['id']}"
        )
        assert res.status_code == 422
        assert res.json()["detail"] == (
            "Resource does not exist or does not belong to project"
        )


async def test_delete_dataset_failure(
    db,
    MockCurrentUser,
    project_factory,
    job_factory,
    tmp_path,
    workflow_factory,
    dataset_factory,
    task_factory,
    client,
):
    """
    GIVEN a Dataset in a relationship with an ApplyWorkflow
    WHEN we try to DELETE that Dataset via the correspondig endpoint
    THEN we fail with a 422
    """
    async with MockCurrentUser(persist=True) as user:

        # Populate the database with the appropriate objects
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)
        task = await task_factory(name="task", source="source")
        await workflow.insert_task(task_id=task.id, db=db)
        input_ds = await dataset_factory(project)
        output_ds = await dataset_factory(project)
        dummy_ds = await dataset_factory(project)

        # Create a job in relationship with input_ds, output_ds and workflow
        job = await job_factory(
            project_id=project.id,
            workflow_id=workflow.id,
            input_dataset_id=input_ds.id,
            output_dataset_id=output_ds.id,
            working_dir=(tmp_path / "some_working_dir").as_posix(),
        )

        # Check that you cannot delete datasets in relationship with a job
        for ds_id in (input_ds.id, output_ds.id):
            res = await client.delete(
                f"api/v1/project/{project.id}/dataset/{ds_id}"
            )
            assert res.status_code == 422
            assert f"still linked to job {job.id}" in res.json()["detail"]

        # Test successful dataset deletion
        res = await client.delete(
            f"api/v1/project/{project.id}/dataset/{dummy_ds.id}"
        )
        assert res.status_code == 204
