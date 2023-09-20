import pytest

from fractal_server.common.schemas import ProjectCreate
from fractal_server.common.schemas import ResourceCreate
from fractal_server.common.schemas import TaskUpdate
from fractal_server.common.schemas import WorkflowTaskCreate


def test_fail_valstr():
    ProjectCreate(name="  valid    name ")
    with pytest.raises(ValueError):
        ProjectCreate(name=None)
    with pytest.raises(ValueError):
        ProjectCreate(name="   ")

    TaskUpdate(version=None)
    with pytest.raises(ValueError):
        TaskUpdate(version="   ")


def test_fail_val_absolute_path():
    ResourceCreate(path="/valid/path")
    with pytest.raises(ValueError):
        ResourceCreate(path=None)
    with pytest.raises(ValueError):
        ResourceCreate(path="./invalid/path")


def test_fail_valint():
    WorkflowTaskCreate(order=1)
    with pytest.raises(ValueError):
        WorkflowTaskCreate(order=None)
    with pytest.raises(ValueError):
        WorkflowTaskCreate(order=-1)
