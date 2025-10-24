import pytest
from pydantic import ValidationError

from fractal_server.app.schemas import UserUpdateStrict
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import JobCreateV2
from fractal_server.app.schemas.v2 import ProjectCreateV2
from fractal_server.app.schemas.v2 import TaskCollectPipV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskDumpV2
from fractal_server.app.schemas.v2 import TaskUpdateV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskDumpV2


def test_extra_on_create_models():
    # Dataset
    DatasetCreateV2(name="name", zarr_dir="/zarr/dir")
    with pytest.raises(ValidationError):
        DatasetCreateV2(name="name", zarr_dir="/zarr/dir", foo="bar")

    # Job
    JobCreateV2()
    with pytest.raises(ValidationError):
        JobCreateV2(foo="bar")

    # Project
    ProjectCreateV2(name="name")
    with pytest.raises(ValidationError):
        ProjectCreateV2(name="name", foo="bar")

    # Task
    TaskCreateV2(name="name", command_parallel="cmd")
    with pytest.raises(ValidationError):
        TaskCreateV2(name="name", command_parallel="cmd", foo="bar")

    # Workflow
    WorkflowCreateV2(name="name")
    with pytest.raises(ValidationError):
        WorkflowCreateV2(name="name", foo="bar")

    # WorkflowTask
    WorkflowTaskCreateV2()
    with pytest.raises(ValidationError):
        WorkflowTaskCreateV2(foo="bar")


def test_dictionary_keys_validation():
    args = dict(
        name="name",
        command_non_parallel="cmd",
    )
    with pytest.raises(ValidationError):
        TaskCreateV2(**args, input_types={"": True})
    with pytest.raises(ValidationError):
        TaskCreateV2(**args, input_types={"    ": True})

    assert TaskCreateV2(**args, input_types={"    a": True}).input_types == {
        "a": True
    }

    with pytest.raises(ValidationError):
        TaskCreateV2(**args, input_types={"a": True, "  a   ": False})

    with pytest.raises(
        ValidationError, match="Task must have at least one valid command"
    ):
        TaskCreateV2(name="name")


def test_task_collect_pip():
    TaskCollectPipV2(package="x")
    TaskCollectPipV2(package="/tmp/x.whl")


def test_task_update():
    t = TaskUpdateV2()
    assert t.input_types is None
    assert t.output_types is None
    with pytest.raises(ValidationError):
        TaskUpdateV2(input_types=None)
    with pytest.raises(ValidationError):
        TaskUpdateV2(output_types=None)
    with pytest.raises(ValidationError):
        TaskUpdateV2(name="cannot set name")


def test_job_create():
    JobCreateV2()
    JobCreateV2(last_task_index=None)
    JobCreateV2(last_task_index=0)
    JobCreateV2(last_task_index=1)
    with pytest.raises(ValidationError):
        JobCreateV2(last_task_index=-1)


def test_workflow_task_dump():
    WorkflowTaskDumpV2(
        id=1,
        workflow_id=1,
        type_filters={},
        task_id=1,
        task=TaskDumpV2(
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
