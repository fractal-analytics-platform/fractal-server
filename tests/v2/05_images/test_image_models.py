import pytest
from pydantic import ValidationError

from fractal_server.images import Filters
from fractal_server.images import SingleImage
from fractal_server.images import SingleImageTaskOutput


def test_single_image():

    with pytest.raises(ValidationError):
        SingleImage()

    assert SingleImage(zarr_url="/somewhere").zarr_url == "/somewhere"

    assert SingleImage(zarr_url="/somewhere", origin="foo").origin == "foo"
    assert SingleImage(zarr_url="/somewhere", origin=None).origin is None
    assert SingleImage(zarr_url="/somewhere", origin=3).origin == "3"
    assert SingleImage(zarr_url="/somewhere", origin=True).origin == "True"

    valid_attributes = dict(a="string", b=3, c=0.33, d=True)
    assert (
        SingleImage(
            zarr_url="/somewhere", attributes=valid_attributes
        ).attributes
        == valid_attributes
    )
    invalid_attributes = [
        dict(a=None),
        dict(a=["l", "i", "s", "t"]),
        dict(a={"d": "i", "c": "t"}),
    ]
    for attr in invalid_attributes:
        with pytest.raises(ValidationError):
            SingleImage(zarr_url="/somewhere", attributes=attr)

    valid_types = dict(a=True, b=False)
    assert (
        SingleImage(zarr_url="/somewhere", types=valid_types).types
        == valid_types
    )

    invalid_types = dict(a="not a bool")
    with pytest.raises(ValidationError):
        SingleImage(zarr_url="/somewhere", types=invalid_types)


def test_single_image_task_output():
    valid_attributes = dict(a="string", b=3, c=0.33, d=True, f=None)
    assert (
        SingleImageTaskOutput(
            zarr_url="/somewhere", attributes=valid_attributes
        ).attributes
        == valid_attributes
    )
    invalid_attributes = [
        dict(a=["l", "i", "s", "t"]),
        dict(a={"d": "i", "c": "t"}),
    ]
    for attr in invalid_attributes:
        with pytest.raises(ValidationError):
            SingleImageTaskOutput(zarr_url="/somewhere", attributes=attr)


def test_filters():

    Filters()

    valid_attributes = dict(a="string", b=3, c=0.33, d=True, e=None)
    assert Filters(attributes=valid_attributes).attributes == valid_attributes

    invalid_attributes = [
        dict(a=["l", "i", "s", "t"]),
        dict(a={"d": "i", "c": "t"}),
    ]
    for attr in invalid_attributes:
        with pytest.raises(ValidationError):
            Filters(attributes=attr)

    valid_types = dict(a=True, b=False)
    assert Filters(types=valid_types).types == valid_types

    invalid_types = dict(a="not a bool")
    with pytest.raises(ValidationError):
        Filters(types=invalid_types)
