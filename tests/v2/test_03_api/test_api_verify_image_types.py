from fractal_server.images import SingleImage


async def test_verify_image_types(
    db,
    MockCurrentUser,
    project_factory_v2,
    dataset_factory_v2,
    client,
):
    ZARR_DIR = "/zarr_dir"

    images = []
    index = 0
    well = "B03"
    for is_3D in [True, False, "unset"]:
        types = {"is_3D": is_3D} if is_3D != "unset" else {}
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types=types,
            ).model_dump()
        )
        index += 1
    for illum_corr in [True, "unset"]:
        types = {"illum_corr": illum_corr} if illum_corr != "unset" else {}
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types=types,
            ).model_dump()
        )
        index += 1
    for registered in [False, "unset"]:
        types = {"registered": registered} if registered != "unset" else {}
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types=types,
            ).model_dump()
        )
        index += 1
    well = "B04"
    for bad_type_3 in [True, False]:
        images.append(
            SingleImage(
                zarr_url=f"{ZARR_DIR}/{index}",
                attributes={"well": well},
                types={"bad_type_3": bad_type_3},
            ).model_dump()
        )
        index += 1

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)

    dataset = await dataset_factory_v2(
        project_id=project.id, zarr_dir=ZARR_DIR, images=images
    )

    url = (
        f"api/v2/project/{project.id}/dataset/{dataset.id}/"
        "images/verify-unique-types/"
    )

    # No filters
    res = await client.post(url)
    assert res.status_code == 200
    assert res.json() == ["bad_type_3", "illum_corr", "is_3D"]

    # Attribute filter
    res = await client.post(
        url,
        json={"attribute_filters": {"well": ["B03"]}},
    )
    assert res.status_code == 200
    assert res.json() == ["illum_corr", "is_3D"]

    # Attribute&type filters
    res = await client.post(
        url,
        json={
            "attribute_filters": {"well": ["B03"]},
            "type_filters": {"is_3D": False},
        },
    )
    assert res.status_code == 200
    assert res.json() == ["illum_corr"]

    # Attribute&type filters
    res = await client.post(
        url,
        json={
            "attribute_filters": {"well": ["B03"]},
            "type_filters": {"is_3D": True},
        },
    )
    assert res.status_code == 200
    assert res.json() == []
