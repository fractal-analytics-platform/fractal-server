from datetime import datetime

import pytest

from fractal_server.common.schemas.applyworkflow import ApplyWorkflowRead
from fractal_server.common.schemas.manifest import ManifestV1
from fractal_server.common.schemas.project import ProjectCreate
from fractal_server.common.schemas.project import ResourceCreate
from fractal_server.common.schemas.workflow import WorkflowTaskCreate


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
    )

    assert isinstance(x.start_timestamp, datetime)
    y = x.sanitised_dict()
    assert isinstance(y["start_timestamp"], str)


async def test_manifest():
    ManifestV1(manifest_version="1", task_list=[])
    with pytest.raises(ValueError):
        ManifestV1(manifest_version="2", task_list=[])
