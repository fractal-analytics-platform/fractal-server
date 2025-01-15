import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.images import SingleImage
from fractal_server.images.tools import filter_image_list

IMAGES = [
    SingleImage(
        zarr_url="/tmp/plate.zarr/A/01/0",
        types={"3D": True},
        attributes=dict(
            plate="plate.zarr",
            well="A01",
        ),
    ),
    SingleImage(
        zarr_url="/tmp/plate.zarr/A/02/0",
        types={"3D": True},
        attributes=dict(
            plate="plate.zarr",
            well="A02",
        ),
    ),
    SingleImage(
        zarr_url="/tmp/plate.zarr/A/01/0_corr",
        types={
            "3D": True,
            "illumination_correction": True,
        },
        attributes=dict(
            plate="plate.zarr",
            well="A01",
        ),
    ),
    SingleImage(
        zarr_url="/tmp/plate.zarr/A/02/0_corr",
        types={
            "3D": True,
            "illumination_correction": True,
        },
        attributes=dict(
            plate="plate.zarr",
            well="A02",
        ),
    ),
    SingleImage(
        zarr_url="/tmp/plate_2d.zarr/A/01/0_corr",
        types={
            "3D": False,
            "illumination_correction": True,
        },
        attributes=dict(
            plate="plate_2d.zarr",
            well="A01",
        ),
    ),
    SingleImage(
        zarr_url="/tmp/plate_2d.zarr/A/02/0_corr",
        types={
            "3D": False,
            "illumination_correction": True,
        },
        attributes=dict(
            plate="plate_2d.zarr",
            well="A02",
        ),
    ),
]
IMAGES = [img.dict() for img in IMAGES]


def test_singleimage_attributes_validation():
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
        with pytest.raises(ValidationError):
            SingleImage(zarr_url="/xyz", attributes={"key": item})

    valid = ["string", -7, 3.14, True]
    for item in valid:
        assert (
            SingleImage(zarr_url="/xyz", attributes={"key": item}).attributes[
                "key"
            ]
            == item
        )


@pytest.mark.parametrize(
    "attribute_filters,type_filters,expected_number",
    [
        # No filter
        ({}, {}, 6),
        # Key is not part of attribute keys
        ({"missing_key": ["whatever"]}, {}, 0),
        # Key is not part of type keys (default is False)
        ({}, {"missing_key": True}, 0),
        ({}, {"missing_key": False}, 6),
        # Key is part of attribute keys, but value is missing
        ({"plate": ["missing_plate.zarr"]}, {}, 0),
        # Meaning of None for attributes: skip a given filter
        ({"plate": None}, {}, 6),
        # Single type filter
        ({}, {"3D": True}, 4),
        # Single type filter
        ({}, {"3D": False}, 2),
        # Two type filters
        ({}, {"3D": True, "illumination_correction": True}, 2),
        # Both attribute and type filters
        (
            {"plate": ["plate.zarr"]},
            {"3D": True, "illumination_correction": True},
            2,
        ),
        # Both attribute and type filters
        (
            {"plate": ["plate_2d.zarr"]},
            {"3D": True, "illumination_correction": True},
            0,
        ),
        # Both attribute and type filters
        (
            {"plate": ["plate.zarr"], "well": ["A01"]},
            {"3D": True, "illumination_correction": True},
            1,
        ),
        # Single attribute filter
        ({"well": ["A01"]}, {}, 3),
    ],
)
def test_filter_image_list_SingleImage(
    attribute_filters,
    type_filters,
    expected_number,
):
    filtered_list = filter_image_list(
        images=IMAGES,
        attribute_filters=attribute_filters,
        type_filters=type_filters,
    )

    debug(attribute_filters)
    debug(type_filters)
    debug(filtered_list)
    assert len(filtered_list) == expected_number
