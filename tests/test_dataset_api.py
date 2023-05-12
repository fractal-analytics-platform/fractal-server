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
