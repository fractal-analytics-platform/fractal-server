import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.common.schemas import TaskImport
from fractal_server.common.schemas import TaskRead
from fractal_server.common.schemas import WorkflowCreate
from fractal_server.common.schemas import WorkflowImport
from fractal_server.common.schemas import WorkflowRead
from fractal_server.common.schemas import WorkflowTaskCreate
from fractal_server.common.schemas import WorkflowTaskImport
from fractal_server.common.schemas import WorkflowTaskRead
from fractal_server.common.schemas import WorkflowTaskUpdate
from fractal_server.common.schemas import WorkflowUpdate


def test_workflow_task_create():
    # Successful creation
    t = WorkflowTaskCreate(order=1)
    debug(t)
    # Invalid arguments
    with pytest.raises(ValidationError):
        WorkflowTaskCreate(order=-1)
    with pytest.raises(ValidationError):
        WorkflowTaskCreate(order=None)


def test_workflow_task_update():
    # Successful creation
    t = WorkflowTaskUpdate(meta=dict(something="else"))
    # Forbidden key-value update
    with pytest.raises(ValidationError):
        t = WorkflowTaskUpdate(meta=dict(parallelization_level="new"))
    debug(t)


def test_workflow_create():
    w = WorkflowCreate(name="workflow")
    debug(w)


def test_workflow_import():
    # Successful creation
    t = TaskImport(name="name", source="source")
    wft = WorkflowTaskImport(task=t)
    w = WorkflowImport(name="workflow", task_list=[wft])
    debug(w)
    # Empty-string argument
    with pytest.raises(ValidationError):
        WorkflowImport(name=" ", task_list=[wft])


def test_workflow_read_empty_task_list():
    w = WorkflowRead(id=1, name="workflow", project_id=1, task_list=[])
    debug(w)


def test_workflow_read_non_empty_task_list():
    # Create a TaskRead
    t1 = TaskRead(
        id=9,
        name="name",
        source="source",
        command="command",
        input_type="input_type",
        output_type="output_type",
        meta=dict(something="else"),
    )
    # Create two WorkflowTaskRead
    wft1 = WorkflowTaskRead(id=1, task_id=1, workflow_id=1, task=t1)
    wft2 = WorkflowTaskRead(id=2, task_id=1, workflow_id=1, task=t1)
    # Create a WorkflowRead
    w = WorkflowRead(
        id=1, name="workflow", project_id=1, task_list=[wft1, wft2]
    )
    debug(w)


def test_workflow_update():
    WorkflowUpdate(name="workflow", reordered_workflowtask_ids=[0, 1, 3, 2])
    WorkflowUpdate(name="workflow")
    WorkflowUpdate(reordered_workflowtask_ids=[0, 1, 3, 2])
    with pytest.raises(ValidationError):
        WorkflowUpdate(name="workflow", reordered_workflowtask_ids=[1, 3, 1])
    with pytest.raises(ValidationError):
        WorkflowUpdate(name="workflow", reordered_workflowtask_ids=[1, 3, -1])
