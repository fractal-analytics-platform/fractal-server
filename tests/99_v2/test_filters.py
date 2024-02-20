from fractal_server.app.runner.v2.images import _filter_image_list
from fractal_server.app.runner.v2.images import SingleImage

images = [
    SingleImage(
        path="plate.zarr/A/01/0",
        attributes=dict(
            plate="plate.zarr",
            well="A/01",
            data_dimensionality=3,
        ),
    ),
    SingleImage(
        path="plate.zarr/A/02/0",
        attributes=dict(
            plate="plate.zarr",
            well="A/02",
            data_dimensionality=3,
        ),
    ),
    SingleImage(
        path="plate.zarr/A/01/0_corr",
        attributes=dict(
            plate="plate.zarr",
            well="A/01",
            data_dimensionality=3,
            illumination_correction=True,
        ),
    ),
    SingleImage(
        path="plate.zarr/A/02/0_corr",
        attributes=dict(
            plate="plate.zarr",
            well="A/02",
            data_dimensionality=3,
            illumination_correction=True,
        ),
    ),
    SingleImage(
        path="plate_2d.zarr/A/01/0_corr",
        attributes=dict(
            plate="plate_2d.zarr",
            well="A/01",
            data_dimensionality=2,
            illumination_correction=True,
        ),
    ),
    SingleImage(
        path="plate_2d.zarr/A/02/0_corr",
        attributes=dict(
            plate="plate_2d.zarr",
            well="A/02",
            data_dimensionality=2,
            illumination_correction=True,
        ),
    ),
]


def test_filter_image_list():

    filters = dict(invalid=True)
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 0

    filters = dict(invalid=False)
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 0

    filters = dict(data_dimensionality=3)
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 4

    filters = dict(data_dimensionality=2)
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 2

    filters = dict(data_dimensionality=3, illumination_correction=True)
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 2

    filters = dict(
        data_dimensionality=3,
        illumination_correction=True,
        plate="plate_2d.zarr",
    )
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 0

    filters = dict(
        data_dimensionality=3, illumination_correction=True, plate="plate.zarr"
    )
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 2

    filters = dict(
        data_dimensionality=3,
        illumination_correction=True,
        plate="plate.zarr",
        well="A/01",
    )
    filtered_list = _filter_image_list(images=images, filters=filters)
    assert len(filtered_list) == 1
