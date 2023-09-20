import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.common.schemas import DatasetCreate
from fractal_server.common.schemas import DatasetRead
from fractal_server.common.schemas import DatasetUpdate
from fractal_server.common.schemas import ResourceRead


def test_dataset_create():
    # Successful creation
    d = DatasetCreate(name="name")
    # Successful sanification of whitespaces
    NAME = "name"
    d = DatasetCreate(name=f"   {NAME}   ")
    assert d.name == NAME
    assert not d.read_only  # Because of default False value
    # Missing argument
    with pytest.raises(ValidationError):
        d = DatasetCreate()
    # Empty-string argument
    with pytest.raises(ValidationError):
        d = DatasetCreate(name="  ")


def test_dataset_read():
    # Successful creation - empty resource_list
    d = DatasetRead(
        id=1, project_id=1, resource_list=[], name="n", read_only=True
    )
    debug(d)
    # Successful creation - non-trivial resource_list
    r1 = ResourceRead(id=1, dataset_id=1, path="/something")
    r2 = ResourceRead(id=1, dataset_id=1, path="/something")
    rlist = [r1, r2]
    d = DatasetRead(
        id=1, project_id=1, resource_list=rlist, name="n", read_only=False
    )
    debug(d)


def test_dataset_update():
    # Sanity check: attributes which are not set explicitly are not listed when
    # exclude_unset=True

    payload = dict(name="name")
    dataset_update_dict = DatasetUpdate(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()

    payload = dict(type="type")
    dataset_update_dict = DatasetUpdate(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()

    payload = dict(read_only=True)
    dataset_update_dict = DatasetUpdate(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()

    payload = dict(read_only=True, name="name")
    dataset_update_dict = DatasetUpdate(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()
