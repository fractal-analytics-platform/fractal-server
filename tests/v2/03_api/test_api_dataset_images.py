from devtools import debug

from fractal_server.images import Filters
from fractal_server.images import SingleImage
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.images.tools import match_filter

PREFIX = "api/v2"


def n_images(n: int) -> list[SingleImage]:
    return [
        SingleImage(
            zarr_url=f"/{i}",
            attributes={
                str(i): i,
                "string_attribute": str(i % 2),
                "int_attribute": i % 2,
            },
            types={
                str(i): bool(i % 2),
                "flag": bool(i % 2 + 1),
            },
        ).dict()
        for i in range(n)
    ]


def assert_expected_attributes_and_flags(res, tot_images: int):
    for attribute, values in res.json()["attributes"].items():
        if attribute == "string_attribute":
            assert set(values) == set(["0", "1"])
        elif attribute == "int_attribute":
            assert set(values) == set([0, 1])
        else:
            assert values == [int(values[0])]

    assert set(res.json()["types"]) == set(
        [str(i) for i in range(tot_images)] + ["flag"]
    )


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

    assert "images" not in res.json()

    # query all images
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == N
    assert_expected_attributes_and_flags(res, N)

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
    assert_expected_attributes_and_flags(res, N)

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
    assert_expected_attributes_and_flags(res, N)

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
    assert_expected_attributes_and_flags(res, N)

    # use `page` too large
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        f"?page_size={page_size}&page={last_page+1}"
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N
    assert res.json()["current_page"] == last_page
    assert res.json()["page_size"] == page_size
    assert len(res.json()["images"]) == N % page_size
    assert_expected_attributes_and_flags(res, N)

    # use `query.zarr_url`
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/",
        json=dict(zarr_url=images[3]["zarr_url"]),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == 1
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == 1
    assert len(res.json()["images"]) == 1
    assert_expected_attributes_and_flags(res, N)

    # use `query.attributes`
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/",
        json=dict(filters=dict(types=dict(flag=False))),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == len(
        [
            image
            for image in images
            if match_filter(image, Filters(types={"flag": False}))
        ]
    )
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == res.json()["total_count"]
    assert len(res.json()["images"]) == res.json()["total_count"]
    assert_expected_attributes_and_flags(res, N)

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=1000",
        json=dict(filters=dict(types={"flag": True})),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == len(
        [
            image
            for image in images
            if match_filter(image, filters=Filters(types={"flag": 1}))
        ]
    )
    assert res.json()["page_size"] == 1000
    assert len(res.json()["images"]) == res.json()["total_count"]

    # Filter with non-existing type
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=42",
        json=dict(filters=dict(types={"foo": True})),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == 0
    assert res.json()["page_size"] == 42
    assert res.json()["current_page"] == 1
    assert res.json()["images"] == []
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/",
        json=dict(filters=dict(types={"foo": False})),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == N

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page=-1"
    )
    assert res.status_code == 422
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=0"
    )
    assert res.status_code == 422
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=-1"
    )
    assert res.status_code == 422


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
            f"?zarr_url={image['zarr_url']}",
        )
        assert res.status_code == 204
        res = await client.post(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        )
        assert res.json()["total_count"] == len(IMAGES) - i - 1


async def test_post_new_image(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
):
    N = 10
    images = n_images(N)

    new_image = SingleImage(
        zarr_url="/new_zarr_url",
        attributes={"new_attribute": "xyz"},
        types={"new_type": True},
    ).dict()
    invalid_new_image = SingleImage(zarr_url=images[-1]["zarr_url"]).dict()
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(project_id=project.id, images=images)

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
    )
    assert res.json()["total_count"] == N
    assert "new_attribute" not in res.json()["attributes"].keys()
    assert "new_type" not in res.json()["types"]

    # add ivalid new image
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        json=invalid_new_image,
    )
    assert res.status_code == 422
    debug(res.json())
    # add new image
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        json=new_image,
    )
    assert res.status_code == 201

    # assert changes
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
    )
    assert res.json()["total_count"] == N + 1
    assert "new_attribute" in res.json()["attributes"].keys()
    assert "new_type" in res.json()["types"]


async def test_patch_images(
    MockCurrentUser,
    client,
    project_factory_v2,
    dataset_factory_v2,
    db,
):
    IMAGES = n_images(42)
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
    dataset = await dataset_factory_v2(project_id=project.id, images=IMAGES)
    for image in dataset.images:
        res = await client.patch(
            f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
            json=dict(
                zarr_url=image["zarr_url"],
                origin="/orig",
                attributes={"a": "b"},
                types={"c": True, "d": False},
            ),
        )
        assert res.status_code == 200
        await db.refresh(dataset)
        ret = find_image_by_zarr_url(
            images=dataset.images, zarr_url=image["zarr_url"]
        )
        assert ret["image"]["attributes"] == {"a": "b"}
        assert ret["image"]["types"] == {"c": True, "d": False}
