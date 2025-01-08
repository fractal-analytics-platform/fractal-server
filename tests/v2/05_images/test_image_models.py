import pytest
from pydantic import ValidationError

from fractal_server.images import SingleImage
from fractal_server.images import SingleImageTaskOutput
from fractal_server.images import SingleImageUpdate
from fractal_server.images.models import SingleImageBase


def test_single_image():

    with pytest.raises(ValidationError):
        SingleImage()

    assert SingleImage(zarr_url="/somewhere").zarr_url == "/somewhere"

    assert SingleImage(zarr_url="/somewhere", origin="/foo").origin == "/foo"
    assert SingleImage(zarr_url="/somewhere", origin=None).origin is None

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


def test_url_normalization():

    # zarr_url
    assert SingleImage(zarr_url="/valid/url").zarr_url == "/valid/url"
    assert SingleImage(zarr_url="/remove/slash/").zarr_url == "/remove/slash"

    with pytest.raises(ValidationError) as e:
        SingleImage(zarr_url="s3/foo")
    assert "S3 handling" in e._excinfo[1].errors()[0]["msg"]

    with pytest.raises(ValidationError) as e:
        SingleImage(zarr_url="https://foo.bar")
    assert "URLs must begin" in e._excinfo[1].errors()[0]["msg"]

    # origin
    assert SingleImage(zarr_url="/valid/url", origin=None).origin is None
    assert (
        SingleImage(zarr_url="/valid/url", origin="/valid/origin").origin
        == "/valid/origin"
    )
    assert (
        SingleImage(zarr_url="/valid/url", origin="/remove/slash//").origin
        == "/remove/slash"
    )
    with pytest.raises(ValidationError) as e:
        SingleImage(zarr_url="/valid/url", origin="s3/foo")
    assert "S3 handling" in e._excinfo[1].errors()[0]["msg"]
    with pytest.raises(ValidationError) as e:
        SingleImage(zarr_url="/valid/url", origin="http://foo.bar")
    assert "URLs must begin" in e._excinfo[1].errors()[0]["msg"]


def test_single_image_task_output():
    base = SingleImageBase(zarr_url="/zarr/url", attributes={"x": None})

    # SingleImageTaskOutput accepts 'None' as value
    SingleImageTaskOutput(**base.dict())
    # SingleImage does not accept 'None' as value
    with pytest.raises(ValidationError):
        SingleImage(**base.dict())


def test_single_image_update():

    with pytest.raises(ValidationError):
        SingleImageUpdate()
    SingleImageUpdate(zarr_url="/something")

    # override SingleImageBase validation
    args = dict(zarr_url="/something", attributes=None)
    with pytest.raises(ValidationError):
        SingleImageBase(**args)
    SingleImageUpdate(**args)

    args = dict(zarr_url="/something", types=None)
    with pytest.raises(ValidationError):
        SingleImageBase(**args)
    SingleImageUpdate(**args)

    with pytest.raises(ValidationError):
        SingleImageUpdate(
            zarr_url="/something", attributes={"invalid": ["l", "i", "s", "t"]}
        )
