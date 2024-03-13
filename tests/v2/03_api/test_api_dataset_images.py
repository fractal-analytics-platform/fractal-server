from devtools import debug

from fractal_server.images import SingleImage

PREFIX = "api/v2"


async def test_get_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):

    images = [
        SingleImage(path="/abc", attributes={"a": 1, "b": 2, "c": 3}),
        SingleImage(path="/abc", attributes={"x": 1, "y": 2, "z": 3}),
        SingleImage(path="/abc", attributes={"x": 1, "b": 2, "c": 3}),
        SingleImage(path="/abc", attributes={"a": 1, "y": 2, "c": 3}),
        SingleImage(path="/abd", attributes={"a": 1, "b": 2, "z": 3}),
    ]

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(
            project_id=project.id, images=images
        )
        debug(dataset)
    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/"
    )
    assert "images" not in res.json()
    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
    )
    debug(res.json())
    assert res.status_code == 200
    assert len(res.json()["images"]) == len(images)
    assert sorted(res.json()["attributes"]) == ["a", "b", "c", "x", "y", "z"]

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        params=dict(path="/abc"),
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 4

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        params=dict(abc=123),
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 0

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        params=dict(path="/abc", a=1),
    )
    assert res.status_code == 200
    assert len(res.json()["images"]) == 2
