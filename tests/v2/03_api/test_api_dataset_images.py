from devtools import debug

from fractal_server.images import SingleImage

PREFIX = "api/v2"

N = 100
IMAGES = [SingleImage(path=f"/{i}", attributes={str(i): i}) for i in range(N)]


async def test_query_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(project_id=project.id, images=IMAGES)

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/",
    )
    debug(res.json())
    assert "images" not in res.json()

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == len(IMAGES)
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == len(IMAGES)
    assert set(res.json()["attributes"]) == set([str(i) for i in range(100)])


async def test_delete_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(project_id=project.id, images=IMAGES)
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
    )
    assert res.json()["total_count"] == len(IMAGES)

    for i, image in enumerate(IMAGES):
        res = await client.delete(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/"
            f"?path={image.path}",
        )
        assert res.status_code == 204
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        )
        assert res.json()["total_count"] == len(IMAGES) - i - 1
