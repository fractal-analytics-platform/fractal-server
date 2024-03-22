import pytest
from devtools import debug

from fractal_server.app.runner.v2.filters import Filters
from fractal_server.images import SingleImage
from fractal_server.images import val_scalar_dict
from fractal_server.images.tools import _filter_image_list

IMAGES = [
    SingleImage(
        path="plate.zarr/A/01/0",
        types=dict(has_z=True),
        attributes=dict(
            plate="plate.zarr",
            well="A01",
        ),
    ),
    SingleImage(
        path="plate.zarr/A/02/0",
        types=dict(has_z=True),
        attributes=dict(
            plate="plate.zarr",
            well="A02",
        ),
    ),
    SingleImage(
        path="plate.zarr/A/01/0_corr",
        types=dict(
            has_z=True,
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
            has_z=True,
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
            has_z=False,
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
            has_z=False,
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
        None,
        bool,  # type
        lambda x: x,  # function
    ]
    for item in invalid:
        filters = dict(key=item)
        with pytest.raises(ValueError):
            val_scalar_dict("")(filters)

    valid = ["string", -7, 3.14, True]
    for item in valid:
        filters = dict(key=item)
        assert val_scalar_dict("")(filters) == filters


@pytest.mark.parametrize(
    "attribute_filters,type_filters,expected_number",
    [
        # No filter
        ({}, {}, 6),
        # Key is not part of attribute keys
        ({"missing_key": "whatever"}, {}, 0),
        # Key is not part of type keys (default is False)
        ({}, {"missing_key": True}, 0),
        ({}, {"missing_key": False}, 6),
        # Key is part of attribute keys, but value is missing
        ({"plate": "missing_plate.zarr"}, {}, 0),
        # Meaning of None for attributes: skip a given filter
        ({"plate": None}, {}, 6),
        # Single type filter
        ({}, {"has_z": True}, 4),
        # Single type filter
        ({}, {"has_z": False}, 2),
        # Two type filters
        ({}, {"has_z": True, "illumination_correction": True}, 2),
        # Both attribute and type filters
        (
            {"plate": "plate.zarr"},
            {"has_z": True, "illumination_correction": True},
            2,
        ),
        # Both attribute and type filters
        (
            {"plate": "plate_2d.zarr"},
            {"has_z": True, "illumination_correction": True},
            0,
        ),
        # Both attribute and type filters
        (
            {"plate": "plate.zarr", "well": "A01"},
            {"has_z": True, "illumination_correction": True},
            1,
        ),
        # Single attribute filter
        ({"well": "A01"}, {}, 3),
    ],
)
def test_filter_image_list(
    attribute_filters,
    type_filters,
    expected_number,
):
    filtered_list = _filter_image_list(
        images=IMAGES,
        filters=Filters(attributes=attribute_filters, types=type_filters),
    )

    debug(attribute_filters)
    debug(type_filters)
    debug(filtered_list)
    assert len(filtered_list) == expected_number
