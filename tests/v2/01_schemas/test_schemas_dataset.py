import pytest
from devtools import debug
from pydantic import ValidationError

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import DatasetImportV2
from fractal_server.app.schemas.v2 import DatasetReadV2
from fractal_server.app.schemas.v2 import DatasetUpdateV2
from fractal_server.urls import normalize_url


VALID_ATTRIBUTE_FILTERS = (
    {},
    {"key1": []},
    {"key1": ["A"]},
    {"key1": ["A", "B"]},
    {"key1": [1, 2]},
    {"key1": [True, False]},
    {"key1": [1.5, -1.2]},
    {"key1": None},
    {"key1": [1, 2], "key2": ["A", "B"]},
)

INVALID_ATTRIBUTE_FILTERS = (
    {True: ["value"]},  # non-string key
    {1: ["value"]},  # non-string key
    {"key1": 1},  # not a list
    {"key1": True},  # not a list
    {"key1": "something"},  # not a list
    {"key1": [1], " key1": [1]},  # non-unique normalized keys
    {"key1": [None]},  # None value
    {"key1": [1, 1.0]},  # non-homogeneous types
    {"key1": [1, "a"]},  # non-homogeneous types
    {"key1": [[1, 2], [1, 2]]},  # non-scalar type
    # {"key1": [1, True]},  # non-homogeneous types - FIXME unsupported
)


@pytest.mark.parametrize("attribute_filters", VALID_ATTRIBUTE_FILTERS)
def test_valid_attribute_filters(attribute_filters: dict):
    DatasetCreateV2(
        name="x",
        zarr_dir="/x",
        attribute_filters=attribute_filters,
    )


@pytest.mark.parametrize("attribute_filters", INVALID_ATTRIBUTE_FILTERS)
def test_invalid_attribute_filters(attribute_filters: dict):
    debug(attribute_filters)
    with pytest.raises(ValueError) as e:
        DatasetCreateV2(
            name="x",
            zarr_dir="/x",
            attribute_filters=attribute_filters,
        )
    debug(e.value)


async def test_schemas_dataset_v2():

    project = ProjectV2(id=1, name="project")

    with pytest.raises(ValidationError):
        # Non-boolean types
        DatasetCreateV2(name="name", zarr_dir="/zarr", type_filters={"a": "b"})
    # Test zarr_dir=None is valid
    DatasetCreateV2(name="name", zarr_dir=None)

    dataset_create = DatasetCreateV2(
        name="name",
        zarr_dir="/tmp/",
    )
    assert dataset_create.zarr_dir == normalize_url(dataset_create.zarr_dir)

    with pytest.raises(ValidationError):
        DatasetImportV2(name="name", zarr_dir=None)

    dataset_import = DatasetImportV2(
        name="name",
        attribute_filters={"x": [10]},
        zarr_dir="/tmp/",
        images=[{"zarr_url": "/tmp/image/"}],
    )
    assert dataset_import.zarr_dir == normalize_url(dataset_import.zarr_dir)
    assert dataset_import.images[0].zarr_url == normalize_url(
        dataset_import.images[0].zarr_url
    )

    dataset = DatasetV2(
        **dataset_create.dict(), id=1, project_id=project.id, history=[]
    )

    # Read

    DatasetReadV2(**dataset.model_dump(), project=project)

    # Update

    # validation accepts `zarr_dir` as None, but not `name` and `filters`
    DatasetUpdateV2(zarr_dir=None)
    with pytest.raises(ValidationError):
        DatasetUpdateV2(name=None)
    with pytest.raises(ValidationError):
        DatasetUpdateV2(attribute_filters=None)
    with pytest.raises(ValidationError):
        DatasetUpdateV2(type_filters=None)

    dataset_update = DatasetUpdateV2(name="new name", zarr_dir="/zarr/")
    assert not dataset_update.zarr_dir.endswith("/")

    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(dataset, key, value)

    assert dataset.name == "new name"


def test_zarr_dir():

    DatasetCreateV2(name="foo", zarr_dir="/")
    assert (
        DatasetCreateV2(name="foo", zarr_dir="/foo/bar").zarr_dir
        == DatasetCreateV2(name="foo", zarr_dir="   /foo/bar").zarr_dir
        == DatasetCreateV2(name="foo", zarr_dir="/foo/bar   ").zarr_dir
        == "/foo/bar"
    )
    assert (
        DatasetCreateV2(name="foo", zarr_dir="  / foo bar  ").zarr_dir
        == "/ foo bar"
    )

    with pytest.raises(ValidationError):
        DatasetCreateV2(name="foo", zarr_dir="not/absolute")

    DatasetCreateV2(name="foo", zarr_dir="/#special/chars")
