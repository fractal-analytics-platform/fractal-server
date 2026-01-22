from typing import TypeVar

from pydantic import ValidationError

from fractal_server.images.models import SingleImage
from fractal_server.images.models import SingleImageBase
from fractal_server.images.models import SingleImageTaskOutput
from fractal_server.images.models import SingleImageUpdate

T = TypeVar("T")


def image_ok(model: T, **kwargs) -> T:
    return model(**kwargs)


def image_fail(model: T, **kwargs) -> str:
    try:
        model(**kwargs)
        raise AssertionError(f"{model=}, {kwargs=}")
    except ValidationError as e:
        return str(e)


def test_SingleImageBase():
    image_fail(model=SingleImageBase)

    # zarr_url
    image = image_ok(model=SingleImageBase, zarr_url="/x")
    assert image.model_dump() == {
        "zarr_url": "/x",
        "origin": None,
        "attributes": {},
        "types": {},
    }
    image_fail(
        model=SingleImageBase, zarr_url="x"
    )  # see 'test_url_normalization'
    image_fail(model=SingleImageBase, zarr_url=None)

    # origin
    image = image_ok(model=SingleImageBase, zarr_url="/x", origin="/y")
    assert image.origin == "/y"
    image = image_ok(model=SingleImageBase, zarr_url="/x", origin=None)
    assert image.origin is None
    image_fail(model=SingleImageBase, zarr_url="/x", origin="y")
    image_fail(model=SingleImageBase, origin="/y")

    # attributes
    valid_attributes = {
        "int": 1,
        "float": 1.2,
        "string": "abc",
        "bool": True,
        "null": None,
        "list": ["l", "i", "s", "t"],
        "dict": {"d": "i", "c": "t"},
        "function": lambda x: x,
        "type": int,
    }  # Any
    image = image_ok(
        model=SingleImageBase, zarr_url="/x", attributes=valid_attributes
    )
    assert image.attributes == valid_attributes
    invalid_attributes = {
        "repeated": 1,
        "  repeated ": 2,
    }
    image_fail(
        model=SingleImageBase, zarr_url="/x", attributes=invalid_attributes
    )

    # types
    valid_types = {"y": True, "n": False}  # only booleans
    image = image_ok(model=SingleImageBase, zarr_url="/x", types=valid_types)
    assert image.types == valid_types
    image_fail(model=SingleImageBase, zarr_url="/x", types={"a": "not a bool"})
    image_fail(
        model=SingleImageBase, zarr_url="/x", types={"a": True, " a": True}
    )


def test_url_normalization():
    image = image_ok(model=SingleImageBase, zarr_url="/valid/url")
    assert image.zarr_url == "/valid/url"
    image = image_ok(model=SingleImageBase, zarr_url="/remove/slash/")
    assert image.zarr_url == "/remove/slash"
    image = image_ok(model=SingleImageBase, zarr_url="s3://foo")
    assert image.zarr_url == "s3://foo"

    e = image_fail(model=SingleImageBase, zarr_url="s3/foo")
    assert "URLs must begin with '/' or 's3://'" in e
    e = image_fail(model=SingleImageBase, zarr_url="https://foo.bar")
    assert "URLs must begin" in e

    image_ok(model=SingleImageBase, zarr_url="/x", origin=None)
    image_ok(model=SingleImageBase, zarr_url="/x", origin="/y")
    image_ok(model=SingleImageBase, zarr_url="/x", origin="s3://foo")
    image = image_ok(model=SingleImageBase, zarr_url="/x", origin="/y///")
    assert image.origin == "/y"

    e = image_fail(model=SingleImageBase, zarr_url="/x", origin="s3/foo")
    assert "URLs must begin with '/' or 's3://'" in e
    e = image_fail(
        model=SingleImageBase, zarr_url="/x", origin="https://foo.bar"
    )
    assert "URLs must begin" in e


def test_SingleImageTaskOutput():
    image_ok(
        model=SingleImageTaskOutput,
        zarr_url="/x",
        attributes={
            "int": 1,
            "float": 1.2,
            "string": "abc",
            "bool": True,
            "null": None,
        },
    )
    image_fail(
        model=SingleImageTaskOutput,
        zarr_url="/x",
        attributes={"list": ["l", "i", "s", "t"]},
    )
    image_fail(
        model=SingleImageTaskOutput,
        zarr_url="/x",
        attributes={"dict": {"d": "i", "c": "t"}},
    )
    image_fail(
        model=SingleImageTaskOutput,
        zarr_url="/x",
        attributes={"function": lambda x: x},
    )
    image_fail(
        model=SingleImageTaskOutput,
        zarr_url="/x",
        attributes={"type": int},
    )
    image_fail(
        model=SingleImageTaskOutput,
        zarr_url="/x",
        attributes={"repeated": 1, " repeated": 2},
    )


def test_SingleImage():
    image_ok(
        model=SingleImage,
        zarr_url="/x",
        attributes={
            "int": 1,
            "float": 1.2,
            "string": "abc",
            "bool": True,
        },
    )
    image_fail(
        model=SingleImage,
        zarr_url="/x",
        attributes={"null": None},
    )
    image_fail(
        model=SingleImage,
        zarr_url="/x",
        attributes={"list": ["l", "i", "s", "t"]},
    )
    image_fail(
        model=SingleImage,
        zarr_url="/x",
        attributes={"dict": {"d": "i", "c": "t"}},
    )
    image_fail(
        model=SingleImage,
        zarr_url="/x",
        attributes={"function": lambda x: x},
    )
    image_fail(
        model=SingleImage,
        zarr_url="/x",
        attributes={"type": int},
    )
    image_fail(
        model=SingleImage,
        zarr_url="/x",
        attributes={"repeated": 1, " repeated": 2},
    )


def test_SingleImageUpdate():
    image_fail(model=SingleImageUpdate)

    # zarr_url
    image = image_ok(model=SingleImageUpdate, zarr_url="/x")
    assert image.model_dump() == {
        "zarr_url": "/x",
        "attributes": None,
        "types": None,
    }
    image_fail(model=SingleImageUpdate, zarr_url="x")
    image_fail(model=SingleImageUpdate, zarr_url=None)

    # attributes
    valid_attributes = {
        "int": 1,
        "float": 1.2,
        "string": "abc",
        "bool": True,
    }
    image = image_ok(
        model=SingleImageUpdate, zarr_url="/x", attributes=valid_attributes
    )
    assert image.attributes == valid_attributes
    for invalid_attributes in [
        {"null": None},
        {"list": ["l", "i", "s", "t"]},
        {"dict": {"d": "i", "c": "t"}},
        {"function": lambda x: x},
        {"type": int},
        {"repeated": 1, "  repeated ": 2},
    ]:
        image_fail(
            model=SingleImageUpdate,
            zarr_url="/x",
            attributes=invalid_attributes,
        )

    # types
    valid_types = {"y": True, "n": False}  # only booleans
    image = image_ok(model=SingleImageUpdate, zarr_url="/x", types=valid_types)
    assert image.types == valid_types
    image_fail(
        model=SingleImageUpdate, zarr_url="/x", types={"a": "not a bool"}
    )
    image_fail(
        model=SingleImageUpdate, zarr_url="/x", types={"a": True, " a": True}
    )
