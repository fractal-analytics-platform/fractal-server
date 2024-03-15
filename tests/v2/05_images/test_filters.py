import pytest
from devtools import debug

from fractal_server.images import _filter_image_list
from fractal_server.images import SingleImage
from fractal_server.images import val_scalar_dict

IMAGES = [
    SingleImage(
        path="plate.zarr/A/01/0",
        types=dict(data_dimensionality=3),
        attributes=dict(
            plate="plate.zarr",
            well="A01",
        ),
    ),
    SingleImage(
        path="plate.zarr/A/02/0",
        types=dict(data_dimensionality=3),
        attributes=dict(
            plate="plate.zarr",
            well="A02",
        ),
    ),
    SingleImage(
        path="plate.zarr/A/01/0_corr",
        types=dict(
            data_dimensionality=3,
            illumination_correction=True,
        ),
        attributes=dict(
            plate="plate.zarr",
            well="A01",
        ),
    ),
    SingleImage(
        path="plate.zarr/A/02/0_corr",
        types=dict(
            data_dimensionality=3,
            illumination_correction=True,
        ),
        attributes=dict(
            plate="plate.zarr",
            well="A02",
        ),
    ),
    SingleImage(
        path="plate_2d.zarr/A/01/0_corr",
        types=dict(
            data_dimensionality=2,
            illumination_correction=True,
        ),
        attributes=dict(
            plate="plate_2d.zarr",
            well="A01",
        ),
    ),
    SingleImage(
        path="plate_2d.zarr/A/02/0_corr",
        types=dict(
            data_dimensionality=2,
            illumination_correction=True,
        ),
        attributes=dict(
            plate="plate_2d.zarr",
            well="A02",
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


@pytest.mark.parametrize(
    "attribute_filters,type_filters,expected_number",
    [
        # No filter
        (None, None, 6),
        # Key is not part of attribute keys
        ({"missing_key": "whatever"}, None, 0),
        # Key is not part of type keys
        (None, {"missing_key": "whatever"}, 0),
        # Key is part of type keys, but value is missing
        (None, {"data_dimensionality": 0}, 0),
        # Key is part of attribute keys, but value is missing
        ({"plate": "missing_plate.zarr"}, None, 0),
        # Meaning of None for attributes: skip a given filter
        ({"plate": None}, None, 6),
        # Meaning of None for types: require that a value is actually None
        (None, {"data_dimensionality": None}, 0),
        # Single type filter
        (None, {"data_dimensionality": 3}, 4),
        # Single type filter
        (None, {"data_dimensionality": 2}, 2),
        # Two type filters
        (None, {"data_dimensionality": 3, "illumination_correction": True}, 2),
        # Both attribute and type filters
        (
            {"plate": "plate.zarr"},
            {"data_dimensionality": 3, "illumination_correction": True},
            2,
        ),
        # Both attribute and type filters
        (
            {"plate": "plate_2d.zarr"},
            {"data_dimensionality": 3, "illumination_correction": True},
            0,
        ),
        # Both attribute and type filters
        (
            {"plate": "plate.zarr", "well": "A01"},
            {"data_dimensionality": 3, "illumination_correction": True},
            1,
        ),
        # Single attribute filter
        ({"well": "A01"}, None, 3),
    ],
)
def test_filter_image_list(
    attribute_filters,
    type_filters,
    expected_number,
):
    filtered_list = _filter_image_list(
        images=IMAGES,
        attribute_filters=attribute_filters,
        type_filters=type_filters,
    )

    debug(attribute_filters)
    debug(type_filters)
    debug(filtered_list)
    assert len(filtered_list) == expected_number
