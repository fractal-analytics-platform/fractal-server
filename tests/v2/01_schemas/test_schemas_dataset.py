import pytest
from pydantic import ValidationError

from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import DatasetImportV2
from fractal_server.app.schemas.v2 import DatasetReadV2
from fractal_server.app.schemas.v2 import DatasetUpdateV2
from fractal_server.urls import normalize_url


async def test_schemas_dataset_v2():

    project = ProjectV2(id=1, name="project")

    # Create
    with pytest.raises(ValidationError):
        # Non-scalar attribute
        DatasetCreateV2(
            name="name",
            zarr_dir="/zarr",
            filters={"attributes": {"x": [1, 0]}},
        )
    with pytest.raises(ValidationError):
        # Non-boolean types
        DatasetCreateV2(
            name="name", zarr_dir="/zarr", filters={"types": {"a": "b"}}
        )
    # Test zarr_dir=None is valid
    DatasetCreateV2(name="name", zarr_dir=None)

    dataset_create = DatasetCreateV2(
        name="name",
        filters={"attributes_include": {"x": [10]}},
        zarr_dir="/tmp/",
    )
    assert dataset_create.zarr_dir == normalize_url(dataset_create.zarr_dir)

    with pytest.raises(ValidationError):
        DatasetImportV2(name="name", zarr_dir=None)

    dataset_import = DatasetImportV2(
        name="name",
        filters={"attributes_include": {"x": [10]}},
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

    # validation accepts `zarr_dir` and `filters` as None, but not `name`
    DatasetUpdateV2(zarr_dir=None, filters=None)
    with pytest.raises(ValidationError):
        DatasetUpdateV2(name=None, zarr_dir=None, filters=None)

    dataset_update = DatasetUpdateV2(name="new name", zarr_dir="/zarr/")
    assert not dataset_update.zarr_dir.endswith("/")

    for key, value in dataset_update.dict(exclude_unset=True).items():
        setattr(dataset, key, value)

    assert dataset.name == "new name"
