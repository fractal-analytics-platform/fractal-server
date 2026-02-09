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


def test_project_dir_and_zarr_subfolder():
    DatasetCreate(name="foo", project_dir="/")

    a = DatasetCreate(
        name="foo", project_dir="/foo/bar", zarr_subfolder=" zarr "
    )
    b = DatasetCreate(
        name="foo", project_dir="   /foo/bar", zarr_subfolder=" zarr/ "
    )
    c = DatasetCreate(
        name="foo", project_dir="/foo/bar   ", zarr_subfolder="zarr"
    )

    assert a == b == c
    assert a.project_dir, a.zarr_subfolder == ("/foo/bar", "zarr")

    d = DatasetCreate(
        name="foo", project_dir="  / foo bar  ", zarr_subfolder=" za rr "
    )
    assert d.project_dir, d.zarr_subfolder == ("/ foo/ bar", "za rr")

    with pytest.raises(ValidationError):
        # project_dir not absolute
        DatasetCreate(name="foo", project_dir="foo/bar", zarr_subfolder="zarr")

    with pytest.raises(ValidationError):
        # zarr_subfolder absolute
        DatasetCreate(
            name="foo", project_dir="/foo/bar", zarr_subfolder="/zarr"
        )

    with pytest.raises(ValidationError):
        # dot dot
        DatasetCreate(
            name="foo", project_dir="/foo/../bar", zarr_subfolder="zarr"
        )
    with pytest.raises(ValidationError):
        # dot dot
        DatasetCreate(
            name="foo", project_dir="/foo/bar", zarr_subfolder="za/../rr"
        )

    DatasetCreate(
        name="foo", project_dir="/#special/chars", zarr_subfolder="?../#"
    )


def test_regex_validators():
    # test SafeNonEmptyStr
    invalid_names = [
        "name#",
        "na$me",
        "na%me",
        "na&me",
        "na(me)",
        "nice😊name",
        "nàmé",
        "na/me",
    ]
    for name in invalid_names:
        with pytest.raises(ValidationError):
            DatasetCreate(name=name)

    valid_names = [
        "......................",
        "  name  with  spaces  ",
        "name-with-hyphens",
    ]
    for name in valid_names:
        DatasetCreate(name=name)

    # test SafeRelativePathStr
    invalid_zarr_subfolders = [
        "zarr#",
        "za$rr",
        "za%rr",
        "za&rr",
        "za(rr)",
        "nice😊zarr",
        "zàrṛ",
    ]
    for zarr_subfolder in invalid_zarr_subfolders:
        with pytest.raises(ValidationError):
            DatasetCreate(
                name="name", project_dir="/", zarr_subfolder=zarr_subfolder
            )

    valid_zarr_subfolders = [
        "very/long/path",
        "path/......./with/............/dots",
    ]
    for zarr_subfolder in valid_zarr_subfolders:
        DatasetCreate(
            name="name", project_dir="/", zarr_subfolder=zarr_subfolder
        )
