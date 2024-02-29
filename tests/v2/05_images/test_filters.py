import pytest

from fractal_server.images import _filter_image_list
from fractal_server.images import SingleImage
from fractal_server.images import val_scalar_dict

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


def test_filter_validation():
    invalid = [
        ["l", "i", "s", "t"],
        {"d": "i", "c": "t"},
        {"s", "e", "t"},
        ("t", "u", "p", "l", "e"),
        bool,  # type
        lambda x: x,  # function
    ]
    for item in invalid:
        filters = dict(key=item)
        with pytest.raises(ValueError):
            val_scalar_dict("")(filters)

    valid = ["string", -7, 3.14, True, None]
    for item in valid:
        filters = dict(key=item)
        assert val_scalar_dict("")(filters) == filters


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
        data_dimensionality=3,
        illumination_correction=True,
        plate="plate.zarr",
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
