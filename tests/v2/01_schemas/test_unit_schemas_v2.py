import pytest
from pydantic import ValidationError

from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import JobCreateV2
from fractal_server.app.schemas.v2 import ProjectCreateV2
from fractal_server.app.schemas.v2 import TaskCollectPipV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskUpdateV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskDumpV2
from fractal_server.images import Filters


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
    TaskCreateV2(name="name", source="source", command_parallel="cmd")
    with pytest.raises(ValidationError):
        TaskCreateV2(
            name="name", source="source", command_parallel="cmd", foo="bar"
        )

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
        source="source",
        command_non_parallel="cmd",
    )
    with pytest.raises(ValidationError):
        TaskCreateV2(**args, input_types={"": True})
    with pytest.raises(ValidationError):
        TaskCreateV2(**args, input_types={"    ": True})

    assert TaskCreateV2(**args, input_types={"    a": True}).input_types == {
        "a": True
    }
    assert TaskCreateV2(**args, input_types={1: True}).input_types == {
        "1": True
    }

    with pytest.raises(ValidationError):
        TaskCreateV2(**args, input_types={"a": True, "  a   ": False})

    with pytest.raises(
        ValidationError, match="Task must have at least one valid command"
    ):
        TaskCreateV2(name="name", source="source")


def test_task_collect_pip():

    TaskCollectPipV2(package="x")
    TaskCollectPipV2(package="/tmp/x.whl")

    with pytest.raises(ValidationError):
        TaskCollectPipV2(package="/tmp/x.wh")

    with pytest.raises(ValidationError):
        TaskCollectPipV2(package="tmp/x.wh")

    with pytest.raises(ValueError) as e:
        TaskCollectPipV2(package="/tmp/x.whl", package_version="1")
    msg = "Cannot provide package version when package is a wheel file"
    assert msg in str(e.value)


def test_task_update():
    t = TaskUpdateV2()
    assert t.input_types is None
    assert t.output_types is None
    with pytest.raises(ValidationError):
        TaskUpdateV2(input_types=None)
    with pytest.raises(ValidationError):
        TaskUpdateV2(output_types=None)


def test_job_create():
    JobCreateV2()
    JobCreateV2(last_task_index=None)
    JobCreateV2(last_task_index=1)
    with pytest.raises(ValidationError, match="cannot be negative"):
        JobCreateV2(last_task_index=-1)


def test_workflow_task_dump():
    WorkflowTaskDumpV2(
        id=1,
        workflow_id=1,
        input_filters=Filters(),
        task_id=1,
    )
    with pytest.raises(ValidationError, match="none"):
        WorkflowTaskDumpV2(
            id=1,
            workflow_id=1,
            input_filters=Filters(),
        )
