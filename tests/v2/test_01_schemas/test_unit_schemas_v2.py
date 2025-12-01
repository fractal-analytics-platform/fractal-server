import pytest
from pydantic import ValidationError

from fractal_server.app.schemas import UserUpdateStrict
from fractal_server.app.schemas.v2 import DatasetCreate
from fractal_server.app.schemas.v2 import JobCreate
from fractal_server.app.schemas.v2 import ProjectCreate
from fractal_server.app.schemas.v2 import TaskCollectPip
from fractal_server.app.schemas.v2 import TaskCreate
from fractal_server.app.schemas.v2 import TaskDump
from fractal_server.app.schemas.v2 import TaskUpdate
from fractal_server.app.schemas.v2 import WorkflowCreate
from fractal_server.app.schemas.v2 import WorkflowTaskCreate
from fractal_server.app.schemas.v2 import WorkflowTaskDump


def test_extra_on_create_models():
    # Dataset
    DatasetCreate(name="name", zarr_dir="/zarr/dir")
    with pytest.raises(ValidationError):
        DatasetCreate(name="name", zarr_dir="/zarr/dir", foo="bar")

    # Job
    JobCreate()
    with pytest.raises(ValidationError):
        JobCreate(foo="bar")

    # Project
    ProjectCreate(name="name")
    with pytest.raises(ValidationError):
        ProjectCreate(name="name", foo="bar")

    # Task
    TaskCreate(name="name", command_parallel="cmd")
    with pytest.raises(ValidationError):
        TaskCreate(name="name", command_parallel="cmd", foo="bar")

    # Workflow
    WorkflowCreate(name="name")
    with pytest.raises(ValidationError):
        WorkflowCreate(name="name", foo="bar")

    # WorkflowTask
    WorkflowTaskCreate()
    with pytest.raises(ValidationError):
        WorkflowTaskCreate(foo="bar")


def test_dictionary_keys_validation():
    args = dict(
        name="name",
        command_non_parallel="cmd",
    )
    with pytest.raises(ValidationError):
        TaskCreate(**args, input_types={"": True})
    with pytest.raises(ValidationError):
        TaskCreate(**args, input_types={"    ": True})

    assert TaskCreate(**args, input_types={"    a": True}).input_types == {
        "a": True
    }

    with pytest.raises(ValidationError):
        TaskCreate(**args, input_types={"a": True, "  a   ": False})

    with pytest.raises(
        ValidationError, match="Task must have at least one valid command"
    ):
        TaskCreate(name="name")


def test_task_collect_pip():
    TaskCollectPip(package="x")
    TaskCollectPip(package="/tmp/x.whl")


def test_task_update():
    t = TaskUpdate()
    assert t.input_types is None
    assert t.output_types is None
    with pytest.raises(ValidationError):
        TaskUpdate(input_types=None)
    with pytest.raises(ValidationError):
        TaskUpdate(output_types=None)
    with pytest.raises(ValidationError):
        TaskUpdate(name="cannot set name")


def test_job_create():
    JobCreate()
    JobCreate(last_task_index=None)
    JobCreate(last_task_index=0)
    JobCreate(last_task_index=1)
    with pytest.raises(ValidationError):
        JobCreate(last_task_index=-1)


def test_workflow_task_dump():
    WorkflowTaskDump(
        id=1,
        workflow_id=1,
        type_filters={},
        task_id=1,
        task=TaskDump(
            id=1,
            name="name",
            type="parallel",
            input_types={},
            output_types={},
        ),
    )


def test_UserUpdateStrict():
    UserUpdateStrict()
    UserUpdateStrict(slurm_accounts=[])
    UserUpdateStrict(slurm_accounts=["a"])
    UserUpdateStrict(slurm_accounts=["a", "b"])
    with pytest.raises(ValueError, match="has repetitions"):
        UserUpdateStrict(slurm_accounts=["a", "a"])
