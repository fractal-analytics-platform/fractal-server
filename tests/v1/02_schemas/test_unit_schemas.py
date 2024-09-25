from datetime import datetime
from datetime import timezone

import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.app.schemas.v1 import ApplyWorkflowReadV1
from fractal_server.app.schemas.v1 import DatasetReadV1
from fractal_server.app.schemas.v1 import ManifestV1
from fractal_server.app.schemas.v1 import ProjectReadV1
from fractal_server.app.schemas.v1 import ResourceReadV1
from fractal_server.app.schemas.v1 import StateRead
from fractal_server.app.schemas.v1 import TaskCollectPipV1
from fractal_server.app.schemas.v1 import TaskManifestV1
from fractal_server.app.schemas.v1 import TaskReadV1
from fractal_server.app.schemas.v1 import WorkflowReadV1
from fractal_server.app.schemas.v1 import WorkflowTaskReadV1
from fractal_server.app.schemas.v1.dumps import DatasetDumpV1
from fractal_server.app.schemas.v1.dumps import ProjectDumpV1
from fractal_server.app.schemas.v1.dumps import WorkflowDumpV1
from fractal_server.utils import get_timestamp


def test_apply_workflow_read():
    WORKFLOW_DUMP = dict(
        id=1,
        project_id=1,
        name="wf",
        task_list=[],
        timestamp_created=str(get_timestamp()),
    )
    DATASET_DUMP = dict(
        id=1,
        project_id=1,
        name="ds",
        type="zarr",
        read_only=False,
        resource_list=[dict(id=1, dataset_id=1, path="/tmp")],
        timestamp_created=str(get_timestamp()),
    )
    PROJECT_DUMP = dict(
        id=1,
        name="project",
        read_only=False,
        timestamp_created=str(get_timestamp()),
    )
    job1 = ApplyWorkflowReadV1(
        id=1,
        project_id=1,
        workflow_id=1,
        input_dataset_id=1,
        output_dataset_id=1,
        start_timestamp="2019-12-23T23:10:11.115310Z",
        status="good",
        project_dump=PROJECT_DUMP,
        workflow_dump=WORKFLOW_DUMP,
        input_dataset_dump=DATASET_DUMP,
        output_dataset_dump=DATASET_DUMP,
        user_email="test@fractal.com",
    )
    debug(job1)
    debug(job1.start_timestamp)

    assert isinstance(job1.project_dump, ProjectDumpV1)
    assert isinstance(job1.workflow_dump, WorkflowDumpV1)
    assert isinstance(job1.input_dataset_dump, DatasetDumpV1)
    assert isinstance(job1.output_dataset_dump, DatasetDumpV1)

    assert isinstance(job1.start_timestamp, datetime)

    job2 = ApplyWorkflowReadV1(
        id=1,
        start_timestamp="2019-12-23T23:10:11.115310Z",
        status="good",
        project_dump=PROJECT_DUMP,
        workflow_dump=WORKFLOW_DUMP,
        input_dataset_dump=DATASET_DUMP,
        output_dataset_dump=DATASET_DUMP,
        user_email="test@fractal.com",
    )
    assert job2.project_id is None
    assert job2.input_dataset_id is None
    assert job2.output_dataset_id is None
    assert job2.workflow_id is None

    job3 = ApplyWorkflowReadV1(
        id=1,
        start_timestamp=datetime(2000, 1, 1, tzinfo=None),
        end_timestamp=datetime(2000, 1, 2, tzinfo=None),
        status="good",
        project_dump=PROJECT_DUMP,
        workflow_dump=WORKFLOW_DUMP,
        input_dataset_dump=DATASET_DUMP,
        output_dataset_dump=DATASET_DUMP,
        user_email="test@fractal.com",
    )
    assert job3.start_timestamp.tzinfo == timezone.utc  # because of valutc
    assert job3.end_timestamp.tzinfo == timezone.utc  # because of valutc


def test_dataset_read():
    # Successful creation - empty resource_list
    d = DatasetReadV1(
        id=1,
        project_id=1,
        project=ProjectReadV1(
            id=1,
            name="project",
            read_only=False,
            timestamp_created=datetime(1999, 1, 1, tzinfo=None),
        ),
        resource_list=[],
        name="n",
        read_only=True,
        timestamp_created=datetime(2000, 1, 1, tzinfo=None),
    )
    debug(d)
    assert d.timestamp_created.tzinfo == timezone.utc  # because of valutc
    assert d.project.timestamp_created.tzinfo == timezone.utc  # valutc
    # Successful creation - non-trivial resource_list
    r1 = ResourceReadV1(id=1, dataset_id=1, path="/something")
    r2 = ResourceReadV1(id=1, dataset_id=1, path="/something")
    rlist = [r1, r2]
    with pytest.raises(ValidationError):
        # missing "project"
        DatasetReadV1(
            id=1,
            project_id=1,
            resource_list=rlist,
            name="n",
            read_only=False,
            timestamp_created=get_timestamp(),
        )


def test_ManifestV1():
    task_without_args_schema = TaskManifestV1(
        name="Task A",
        executable="executable",
        input_type="input_type",
        output_type="output_type",
        default_args={"arg": "val"},
    )
    task_with_args_schema = TaskManifestV1(
        name="Task B",
        executable="executable",
        input_type="input_type",
        output_type="output_type",
        default_args={"arg": "val"},
        args_schema={"something": "else"},
    )
    task_with_docs_right_link = TaskManifestV1(
        name="Task B",
        executable="executable",
        input_type="input_type",
        output_type="output_type",
        default_args={"arg": "val"},
        args_schema={"something": "else"},
        docs_link="http://www.example.org",
    )

    m = ManifestV1(
        manifest_version="1",
        task_list=[task_without_args_schema],
    )
    debug(m)
    m = ManifestV1(
        manifest_version="1",
        has_args_schemas=False,
        task_list=[task_without_args_schema],
    )
    debug(m)
    m = ManifestV1(
        manifest_version="1",
        has_args_schemas=True,
        task_list=[task_with_args_schema],
    )
    debug(m)
    m = ManifestV1(
        manifest_version="1",
        has_args_schemas=True,
        task_list=[task_with_docs_right_link],
    )
    debug(m)

    with pytest.raises(ValidationError) as e:
        TaskManifestV1(
            name="Task B",
            executable="executable",
            input_type="input_type",
            output_type="output_type",
            default_args={"arg": "val"},
            args_schema={"something": "else"},
            docs_link="htp://www.example.org",
        )
    debug(e.value)

    with pytest.raises(ValidationError) as e:
        m = ManifestV1(
            manifest_version="1",
            task_list=[task_without_args_schema, task_with_args_schema],
            has_args_schemas=True,
        )
    debug(e.value)

    with pytest.raises(ValidationError):
        ManifestV1(manifest_version="2", task_list=[task_with_args_schema])


def test_state_read():
    s = StateRead(
        data={"some": "thing"}, timestamp=datetime(2000, 1, 1, tzinfo=None)
    )
    debug(s)
    assert s.id is None
    assert s.timestamp.tzinfo == timezone.utc  # because of valutc

    s = StateRead(data={"some": "thing"}, timestamp=get_timestamp(), id=1)
    debug(s)
    assert s.id == 1


def test_TaskCollectPip():
    # Successful creation
    TaskCollectPipV1(package="some-package")
    TaskCollectPipV1(package="some-package", package_version="0.0.1")
    TaskCollectPipV1(package="/some/package.whl")
    # Failed creation
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="some/package")
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="some/package.whl")
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="/some/package", package_version=None)
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="/some/package.whl", package_version="0.0.1")
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="/some/package.tar.gz")
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="some-package", package_extras="")
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="some-package", package_extras=None)

    TaskCollectPipV1(package="some-package", pinned_package_versions={})

    TaskCollectPipV1(
        package="some-package",
        pinned_package_versions={"numpy": "1.22.0", "pydantic": "1.10.10"},
    )
    with pytest.raises(ValidationError):
        TaskCollectPipV1(package="some-package", pinned_package_versions=1)


def test_workflow_read_empty_task_list():
    w = WorkflowReadV1(
        id=1,
        name="workflow",
        project_id=1,
        task_list=[],
        project=ProjectReadV1(
            id=1,
            name="project",
            read_only=False,
            timestamp_created=get_timestamp(),
        ),
        timestamp_created=datetime(2000, 1, 1, tzinfo=None),
    )
    debug(w)
    assert w.timestamp_created.tzinfo == timezone.utc  # because of valutc


def test_workflow_read_non_empty_task_list():
    # Create a TaskRead
    t1 = TaskReadV1(
        id=9,
        name="name",
        source="source",
        command="command",
        input_type="input_type",
        output_type="output_type",
        meta=dict(something="else"),
    )
    # Create two WorkflowTaskRead
    wft1 = WorkflowTaskReadV1(id=1, task_id=1, workflow_id=1, task=t1)
    wft2 = WorkflowTaskReadV1(id=2, task_id=1, workflow_id=1, task=t1)
    # Create a WorkflowRead
    w = WorkflowReadV1(
        id=1,
        name="workflow",
        project_id=1,
        task_list=[wft1, wft2],
        project=ProjectReadV1(
            id=1,
            name="project",
            read_only=False,
            timestamp_created=get_timestamp(),
        ),
        timestamp_created=str(get_timestamp()),
    )
    debug(w)
