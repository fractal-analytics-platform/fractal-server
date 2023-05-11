import pytest
from devtools import debug
from fastapi import HTTPException

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

        with pytest.raises(HTTPException) as err:
            res = await client.patch(
                f"{PREFIX}/project/{project.id}/dataset/{other_dataset.id}/"
                f"resource/{resource['id']}",
                json=dict(path="/tmp/abc"),
            )
            debug(res, res.json())
        assert err.value.status_code == 422
        assert err.value.detail == (
            f"Resource {resource['id']} is not part "
            f"of dataset {other_dataset.id}"
        )
