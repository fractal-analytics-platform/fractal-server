import pytest
from pydantic import ValidationError

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import DatasetCreate
from fractal_server.app.schemas.v2 import DatasetImport
from fractal_server.app.schemas.v2 import DatasetRead
from fractal_server.app.schemas.v2 import DatasetUpdate
from fractal_server.urls import normalize_url


async def test_schemas_dataset_v2():
    project = ProjectV2(id=1, name="project")

    # Test zarr_dir=None is valid
    DatasetCreate(name="name", zarr_dir=None)

    dataset_create = DatasetCreate(
        name="name",
        zarr_dir="/tmp/",
    )
    assert dataset_create.zarr_dir == normalize_url(dataset_create.zarr_dir)

    with pytest.raises(ValidationError):
        DatasetImport(name="name", zarr_dir=None)

    dataset_import = DatasetImport(
        name="name",
        zarr_dir="/tmp/",
        images=[{"zarr_url": "/tmp/image/"}],
    )
    assert dataset_import.zarr_dir == normalize_url(dataset_import.zarr_dir)
    assert dataset_import.images[0].zarr_url == normalize_url(
        dataset_import.images[0].zarr_url
    )

    dataset = DatasetV2(
        **dataset_create.model_dump(), id=1, project_id=project.id, history=[]
    )

    # Read

    DatasetRead(**dataset.model_dump(), project=project.model_dump())

    # Update

    # validation accepts `zarr_dir` as None, but not `name`
    DatasetUpdate(zarr_dir=None)
    with pytest.raises(ValidationError):
        DatasetUpdate(name=None)

    dataset_update = DatasetUpdate(name="new name", zarr_dir="/zarr/")
    assert not dataset_update.zarr_dir.endswith("/")

    for key, value in dataset_update.model_dump(exclude_unset=True).items():
        setattr(dataset, key, value)

    assert dataset.name == "new name"


def test_zarr_dir():
    DatasetCreate(name="foo", zarr_dir="/")
    assert (
        DatasetCreate(name="foo", zarr_dir="/foo/bar").zarr_dir
        == DatasetCreate(name="foo", zarr_dir="   /foo/bar").zarr_dir
        == DatasetCreate(name="foo", zarr_dir="/foo/bar   ").zarr_dir
        == "/foo/bar"
    )
    assert (
        DatasetCreate(name="foo", zarr_dir="  / foo bar  ").zarr_dir
        == "/ foo bar"
    )

    with pytest.raises(ValidationError):
        DatasetCreate(name="foo", zarr_dir="not/absolute")

    DatasetCreate(name="foo", zarr_dir="/#special/chars")
