from datetime import datetime

import pytest

from fractal_server.common.schemas import ApplyWorkflowRead
from fractal_server.common.schemas import ManifestV1
from fractal_server.common.schemas import ProjectCreate
from fractal_server.common.schemas import ResourceCreate
from fractal_server.common.schemas import TaskCollectPip
from fractal_server.common.schemas import WorkflowTaskCreate
from fractal_server.common.schemas import WorkflowUpdate


async def test_fail_valstr():
    ProjectCreate(name="  valid    name ")
    with pytest.raises(ValueError):
        ProjectCreate(name=None)
    with pytest.raises(ValueError):
        ProjectCreate(name="   ")


async def test_fail_val_absolute_path():
    ResourceCreate(path="/valid/path")
    with pytest.raises(ValueError):
        ResourceCreate(path=None)
    with pytest.raises(ValueError):
        ResourceCreate(path="./invalid/path")


async def test_fail_valint():
    WorkflowTaskCreate(order=1)
    with pytest.raises(ValueError):
        WorkflowTaskCreate(order=None)
    with pytest.raises(ValueError):
        WorkflowTaskCreate(order=-1)


async def test_apply_wf_read():
    x = ApplyWorkflowRead(
        id=1,
        project_id=1,
        workflow_id=1,
        input_dataset_id=1,
        output_dataset_id=1,
        start_timestamp="2019-12-23T23:10:11.115310Z",
        status="good",
        workflow_dump={
            "name": "my workflow",
            "id": 1,
            "project_id": 1,
            "task_list": [],
        },
    )

    assert isinstance(x.start_timestamp, datetime)
    y = x.sanitised_dict()
    assert isinstance(y["start_timestamp"], str)


async def test_manifest():
    ManifestV1(manifest_version="1", task_list=[])
    with pytest.raises(ValueError):
        ManifestV1(manifest_version="2", task_list=[])


async def test_fail_wfupdate():
    WorkflowUpdate(reordered_workflowtask_ids=[1, 2, 3])
    with pytest.raises(ValueError):
        WorkflowUpdate(reordered_workflowtask_ids=[1, -2, 3])


async def test_non_absolute_path():
    TaskCollectPip(package="/some/path.whl")
    with pytest.raises(ValueError):
        TaskCollectPip(package="a/b/c")
