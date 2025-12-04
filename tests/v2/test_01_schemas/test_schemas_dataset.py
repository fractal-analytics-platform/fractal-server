import os

import pytest
from pydantic import ValidationError

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import DatasetCreate
from fractal_server.app.schemas.v2 import DatasetImport
from fractal_server.app.schemas.v2 import DatasetRead
from fractal_server.app.schemas.v2 import DatasetUpdate
from fractal_server.urls import normalize_url


async def test_schemas_dataset():
    project = ProjectV2(id=1, name="project")

    # Test zarr_dir=None is valid
    DatasetCreate(name="name", project_dir=None)

    dataset_create = DatasetCreate(
        name="name",
        project_dir="/",
        zarr_subfolder="tmp",
    )

    with pytest.raises(ValidationError):
        DatasetCreate(
            name="name",
            project_dir=None,
            zarr_subfolder="something",
        )

    with pytest.raises(ValidationError):
        DatasetCreate(
            name="name",
            project_dir="/",
            zarr_subfolder="/absolute",
        )

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
        **dataset_create.model_dump(),
        id=1,
        project_id=project.id,
        history=[],
        zarr_dir=os.path.join(
            dataset_create.project_dir, dataset_create.zarr_subfolder
        ),
    )

    # Read

    DatasetRead(**dataset.model_dump(), project=project.model_dump())

    # Update

    DatasetUpdate()

    with pytest.raises(ValidationError):
        DatasetUpdate(name=None)

    DatasetUpdate(name="name")


def test_project_dir():
    DatasetCreate(name="foo", project_dir="/")
    assert (
        DatasetCreate(name="foo", project_dir="/foo/bar").project_dir
        == DatasetCreate(name="foo", project_dir="   /foo/bar").project_dir
        == DatasetCreate(name="foo", project_dir="/foo/bar   ").project_dir
        == "/foo/bar"
    )
    assert (
        DatasetCreate(name="foo", project_dir="  / foo bar  ").project_dir
        == "/ foo bar"
    )

    with pytest.raises(ValidationError):
        DatasetCreate(name="foo", project_dir="not/absolute")

    DatasetCreate(name="foo", project_dir="/#special/chars")
