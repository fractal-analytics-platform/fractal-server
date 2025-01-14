from fractal_server.images import SingleImage
from fractal_server.images.tools import find_image_by_zarr_url
from fractal_server.images.tools import match_filter

PREFIX = "api/v2"

ZARR_DIR = "/zarr_dir"


def n_images(n: int) -> list[dict]:
    return [
        SingleImage(
            zarr_url=f"{ZARR_DIR}/{i}",
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

    dataset = await dataset_factory_v2(
        project_id=project.id, zarr_dir=ZARR_DIR, images=images
    )

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
        json=dict(type_filters=dict(flag=False)),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == len(
        [
            image
            for image in images
            if match_filter(
                image=image, type_filters={"flag": False}, attribute_filters={}
            )
        ]
    )
    assert res.json()["current_page"] == 1
    assert res.json()["page_size"] == res.json()["total_count"]
    assert len(res.json()["images"]) == res.json()["total_count"]
    assert_expected_attributes_and_flags(res, N)

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=1000",
        json=dict(type_filters={"flag": True}),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == len(
        [
            image
            for image in images
            if match_filter(
                image=image, type_filters={"flag": 1}, attribute_filters={}
            )
        ]
    )
    assert res.json()["page_size"] == 1000
    assert len(res.json()["images"]) == res.json()["total_count"]

    # Filter with non-existing type
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
        "?page_size=42",
        json=dict(type_filters={"foo": True}),
    )
    assert res.status_code == 200
    assert res.json()["total_count"] == 0
    assert res.json()["page_size"] == 42
    assert res.json()["current_page"] == 1
    assert res.json()["images"] == []
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/",
        json=dict(type_filters={"foo": False}),
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
    IMAGES = n_images(10)
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(
        project_id=project.id, zarr_dir=ZARR_DIR, images=IMAGES
    )
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

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(
        project_id=project.id, zarr_dir=ZARR_DIR, images=images
    )

    new_image = SingleImage(
        zarr_url=f"{ZARR_DIR}/new_zarr_url",
        attributes={"new_attribute": "xyz"},
        types={"new_type": True},
    ).dict()
    invalid_new_image_1 = SingleImage(zarr_url=images[-1]["zarr_url"]).dict()
    invalid_new_image_2 = SingleImage(zarr_url="/foo/bar").dict()
    invalid_new_image_3 = SingleImage(zarr_url=dataset.zarr_dir).dict()

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/query/"
    )
    assert res.json()["total_count"] == N
    assert "new_attribute" not in res.json()["attributes"].keys()
    assert "new_type" not in res.json()["types"]

    # add invalid new images
    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        json=invalid_new_image_1,
    )
    assert res.status_code == 422
    assert "already in" in res.json()["detail"][0]

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        json=invalid_new_image_2,
    )
    assert res.status_code == 422
    assert "not relative to" in res.json()["detail"]

    res = await client.post(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        json=invalid_new_image_3,
    )
    assert res.status_code == 422
    assert "cannot be equal to `Dataset.zarr_dir`" in res.json()["detail"]

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
    IMAGES = n_images(1)
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
    dataset = await dataset_factory_v2(project_id=project.id, images=IMAGES)

    res = await client.patch(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        json=dict(
            zarr_url=IMAGES[0]["zarr_url"],
            attributes={"a": "b"},
            types={"c": True, "d": False},
        ),
    )
    assert res.status_code == 200
    assert res.json()["zarr_url"] == IMAGES[0]["zarr_url"]
    assert res.json()["attributes"] == {"a": "b"}
    assert res.json()["types"] == {"c": True, "d": False}

    await db.refresh(dataset)
    ret = find_image_by_zarr_url(
        images=dataset.images, zarr_url=IMAGES[0]["zarr_url"]
    )
    assert ret["image"]["attributes"] == {"a": "b"}
    assert ret["image"]["types"] == {"c": True, "d": False}
    res = await client.patch(
        f"{PREFIX}/project/{project.id}/dataset/{dataset.id}/images/",
        json=dict(zarr_url="/foo/bar"),
    )
    assert res.status_code == 404
