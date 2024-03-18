from devtools import debug

from fractal_server.images import SingleImage

PREFIX = "api/v2"


def n_images(n: int) -> list[SingleImage]:
    return [
        SingleImage(
            path=f"/{i}",
            attributes={
                str(i): i,
                "flag": i % 2,
            },
        )
        for i in range(n)
    ]


async def test_query_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):
    N = 101
    images = n_images(N)
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(project_id=project.id, images=images)

    res = await client.get(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/",
    )
    debug(res.json())
    assert "images" not in res.json()

    # query all images
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == N
    assert set(res.json()["attributes"]) == set(
        [str(i) for i in range(N)] + ["flag"]
    )

    # use `page_size`
    page_size = 50
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=50"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == 50
    assert len(res.json()["images"]) == 50
    assert set(res.json()["attributes"]) == set(
        [str(i) for i in range(N)] + ["flag"]
    )

    # use `page_size` too large
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        f"?page_size={N+1}"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == N + 1
    assert len(res.json()["images"]) == N
    assert set(res.json()["attributes"]) == set(
        [str(i) for i in range(N)] + ["flag"]
    )

    # use `page_size` and `page`
    last_page = (N // page_size) + 1
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        f"?page_size={page_size}&page={last_page}"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N
    assert res.json()["current_page"] == last_page
    assert res.json()["page_size"] == page_size
    assert len(res.json()["images"]) == N % page_size
    assert set(res.json()["attributes"]) == set(
        [str(i) for i in range(N)] + ["flag"]
    )

    # use `page` too large
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        f"?page_size={page_size}&page={last_page+1}"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N
    assert res.json()["current_page"] == last_page + 1
    assert res.json()["page_size"] == page_size
    assert len(res.json()["images"]) == 0
    assert set(res.json()["attributes"]) == set(
        [str(i) for i in range(N)] + ["flag"]
    )

    # use `query.path`
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/",
        json=dict(path=images[3].path),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == 1
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == 1
    assert len(res.json()["images"]) == 1
    assert set(res.json()["attributes"]) == set(
        [str(i) for i in range(N)] + ["flag"]
    )

    # use `query.attributes`
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/",
        json=dict(attributes={"flag": 0}),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == len(
        [image for image in images if image.match_filter({"flag": 0})]
    )
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == res.json()["total_count"]
    assert len(res.json()["images"]) == res.json()["total_count"]
    assert set(res.json()["attributes"]) == set(
        [str(i) for i in range(N)] + ["flag"]
    )
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=1000",
        json=dict(attributes={"flag": 1}),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == len(
        [image for image in images if image.match_filter({"flag": 1})]
    )
    assert res.json()["page_size"] == 1000
    assert len(res.json()["images"]) == res.json()["total_count"]

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/",
        json=dict(attributes={"flag": 2}),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == 0
    assert res.json()["images"] == []


async def test_delete_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):
    IMAGES = n_images(42)
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
