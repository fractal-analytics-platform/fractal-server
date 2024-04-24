from datetime import datetime
from datetime import timezone

import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.app.schemas.state import _StateBase
from fractal_server.app.schemas.state import StateRead
from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user import UserUpdate
from fractal_server.app.schemas.user import UserUpdateStrict
from fractal_server.app.schemas.v1 import ApplyWorkflowCreateV1
from fractal_server.app.schemas.v1 import ApplyWorkflowReadV1
from fractal_server.app.schemas.v1 import ApplyWorkflowUpdateV1
from fractal_server.app.schemas.v1 import DatasetCreateV1
from fractal_server.app.schemas.v1 import DatasetReadV1
from fractal_server.app.schemas.v1 import DatasetUpdateV1
from fractal_server.app.schemas.v1 import ManifestV1
from fractal_server.app.schemas.v1 import ProjectCreateV1
from fractal_server.app.schemas.v1 import ProjectReadV1
from fractal_server.app.schemas.v1 import ResourceCreateV1
from fractal_server.app.schemas.v1 import ResourceReadV1
from fractal_server.app.schemas.v1 import TaskCollectPipV1
from fractal_server.app.schemas.v1 import TaskCreateV1
from fractal_server.app.schemas.v1 import TaskImportV1
from fractal_server.app.schemas.v1 import TaskManifestV1
from fractal_server.app.schemas.v1 import TaskReadV1
from fractal_server.app.schemas.v1 import TaskUpdateV1
from fractal_server.app.schemas.v1 import WorkflowCreateV1
from fractal_server.app.schemas.v1 import WorkflowImportV1
from fractal_server.app.schemas.v1 import WorkflowReadV1
from fractal_server.app.schemas.v1 import WorkflowTaskCreateV1
from fractal_server.app.schemas.v1 import WorkflowTaskImportV1
from fractal_server.app.schemas.v1 import WorkflowTaskReadV1
from fractal_server.app.schemas.v1 import WorkflowTaskUpdateV1
from fractal_server.app.schemas.v1 import WorkflowUpdateV1
from fractal_server.app.schemas.v1.dumps import DatasetDumpV1
from fractal_server.app.schemas.v1.dumps import ProjectDumpV1
from fractal_server.app.schemas.v1.dumps import WorkflowDumpV1
from fractal_server.utils import get_timestamp


def test_apply_workflow_create():

    # ApplyWorkflowCreate
    valid_args = dict(
        worker_init="worker init",
        first_task_index=1,
        last_task_index=10,
        slurm_account="slurm account",
    )
    ApplyWorkflowCreateV1(**valid_args)
    with pytest.raises(ValueError):
        invalid_args = {**valid_args, "worker_init": " "}
        ApplyWorkflowCreateV1(**invalid_args)
    with pytest.raises(ValueError):
        invalid_args = {**valid_args, "worker_init": None}
        ApplyWorkflowCreateV1(**invalid_args)
    with pytest.raises(ValueError):
        invalid_args = {**valid_args, "first_task_index": -1}
        ApplyWorkflowCreateV1(**invalid_args)
    with pytest.raises(ValueError):
        invalid_args = {**valid_args, "last_task_index": -1}
        ApplyWorkflowCreateV1(**invalid_args)
    with pytest.raises(ValueError):
        invalid_args = {
            **valid_args,
            "first_task_index": 2,
            "last_task_index": 0,
        }
        ApplyWorkflowCreateV1(**invalid_args)


def test_apply_workflow_update():
    for status in ["submitted", "done", "failed"]:
        ApplyWorkflowUpdateV1(status=status)
    with pytest.raises(ValueError):
        ApplyWorkflowUpdateV1(status=" ")
    with pytest.raises(ValueError):
        ApplyWorkflowUpdateV1(status=None)
    with pytest.raises(ValueError):
        ApplyWorkflowUpdateV1(status="foo")


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


def test_dataset_create():
    # Successful creation
    d = DatasetCreateV1(name="name")
    # Successful sanification of whitespaces
    NAME = "name"
    d = DatasetCreateV1(name=f"   {NAME}   ")
    assert d.name == NAME
    assert not d.read_only  # Because of default False value
    # Missing argument
    with pytest.raises(ValidationError):
        d = DatasetCreateV1()
    # Empty-string argument
    with pytest.raises(ValidationError):
        d = DatasetCreateV1(name="  ")


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


def test_dataset_update():
    # Sanity check: attributes which are not set explicitly are not listed when
    # exclude_unset=True

    payload = dict(name="name")
    dataset_update_dict = DatasetUpdateV1(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()

    payload = dict(type="type")
    dataset_update_dict = DatasetUpdateV1(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()

    payload = dict(read_only=True)
    dataset_update_dict = DatasetUpdateV1(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()

    payload = dict(read_only=True, name="name")
    dataset_update_dict = DatasetUpdateV1(**payload).dict(exclude_unset=True)
    debug(dataset_update_dict)
    assert dataset_update_dict.keys() == payload.keys()


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


def test_project_create():
    # Successful creation
    p = ProjectCreateV1(name="my project")
    debug(p)
    # Check that whitespaces are stripped from beginning/end of string
    NAME = "some project name"
    p = ProjectCreateV1(name=f"  {NAME}  ")
    debug(p)
    assert p.name == NAME
    # Fail due to empty string
    with pytest.raises(ValidationError):
        ProjectCreateV1(name="  ")


def test_state():
    _StateBase(data={"some": "thing"}, timestamp=get_timestamp())


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


def test_task_update():
    # Successful creation, with many unset fields
    t = TaskUpdateV1(name="name")
    debug(t)
    assert list(t.dict(exclude_none=True).keys()) == ["name"]
    assert list(t.dict(exclude_unset=True).keys()) == ["name"]
    # Some failures
    with pytest.raises(ValidationError):
        TaskUpdateV1(name="task", version="")
    TaskUpdateV1(name="task", version=None)
    # Successful cretion, with mutliple fields set
    t = TaskUpdateV1(
        name="task",
        version="1.2.3",
        owner="someone",
    )
    debug(t)
    assert t.name
    assert t.version


def test_task_create():
    # Successful creation
    t = TaskCreateV1(
        name="task",
        source="source",
        command="command",
        input_type="input_type",
        output_type="output_type",
        version="1.2.3",
        owner="someone",
    )
    debug(t)
    # Missing arguments
    with pytest.raises(ValidationError):
        TaskCreateV1(name="task", source="source")

    # Bad docs link
    with pytest.raises(ValidationError):
        TaskCreateV1(
            name="task",
            source="source",
            command="command",
            input_type="input_type",
            output_type="output_type",
            version="1.2.3",
            owner="someone",
            docs_link="htp://www.example.org",
        )


def test_user_create():
    # Without slurm_user attribute
    u = UserCreate(email="a@b.c", password="asd")
    debug(u)
    assert u.slurm_user is None
    assert u.slurm_accounts == []
    # With valid slurm_user attribute
    u = UserCreate(email="a@b.c", password="asd", slurm_user="slurm_user")
    assert u.slurm_user
    # With invalid slurm_user attribute
    with pytest.raises(ValidationError):
        UserCreate(email="a@b.c", password="asd", slurm_user="  ")

    # slurm_accounts must be a list of StrictStr without repetitions

    u = UserCreate(email="a@b.c", password="asd", slurm_accounts=["a", "b"])
    assert u.slurm_accounts == ["a", "b"]

    with pytest.raises(ValidationError):
        UserCreate(
            email="a@b.c", password="asd", slurm_accounts=[1, "a", True]
        )

    with pytest.raises(ValidationError):
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["a", {"NOT": "VALID"}],
        )
    with pytest.raises(ValidationError):
        # repetitions
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["foo", "bar", "foo", "rab"],
        )
    with pytest.raises(ValidationError):
        # empty string
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["foo", "     ", "bar"],
        )
    user = UserCreate(
        email="a@b.c",
        password="asd",
        slurm_accounts=["f o o", "  bar "],
    )
    assert user.slurm_accounts == ["f o o", "bar"]
    with pytest.raises(ValidationError):
        # repetition after stripping
        UserCreate(
            email="a@b.c",
            password="asd",
            slurm_accounts=["   foo", "foo    "],
        )

    # With valid cache_dir
    CACHE_DIR = "/xxx"
    u = UserCreate(email="a@b.c", password="asd", cache_dir=f"{CACHE_DIR}   ")
    assert u.cache_dir == CACHE_DIR
    # With invalid cache_dir attribute
    with pytest.raises(ValidationError) as e:
        u = UserCreate(email="a@b.c", password="asd", cache_dir="  ")
    debug(e.value)
    assert "cannot be empty" in e.value.errors()[0]["msg"]
    # With invalid cache_dir attribute
    with pytest.raises(ValidationError) as e:
        u = UserCreate(email="a@b.c", password="asd", cache_dir="xxx")
    debug(e.value)
    assert "must be an absolute path" in e.value.errors()[0]["msg"]
    # With all attributes
    u = UserCreate(
        email="a@b.c",
        password="pwd",
        slurm_user="slurm_user",
        username="username",
        cache_dir="/some/path",
    )
    debug(u)
    assert u.slurm_user
    assert u.cache_dir
    assert u.username
    with pytest.raises(ValidationError) as e:
        UserUpdate(cache_dir=None)


def test_user_update_strict():

    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=[42, "Foo"])
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=["Foo", True])
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts="NOT A LIST")
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=[{"NOT": "VALID"}])
    with pytest.raises(ValidationError):
        UserUpdateStrict(slurm_accounts=["a", "b", "a"])
    UserUpdateStrict(slurm_accounts=None)
    UserUpdateStrict(slurm_accounts=["a", "b", "c"])


def test_fail_valstr():
    ProjectCreateV1(name="  valid    name ")
    with pytest.raises(ValueError):
        ProjectCreateV1(name=None)
    with pytest.raises(ValueError):
        ProjectCreateV1(name="   ")

    TaskUpdateV1(version=None)
    with pytest.raises(ValueError):
        TaskUpdateV1(version="   ")


def test_fail_val_absolute_path():
    ResourceCreateV1(path="/valid/path")
    with pytest.raises(ValueError):
        ResourceCreateV1(path=None)
    with pytest.raises(ValueError):
        ResourceCreateV1(path="./invalid/path")


def test_fail_valint():
    WorkflowTaskCreateV1(order=1)
    with pytest.raises(ValueError):
        WorkflowTaskCreateV1(order=None)
    with pytest.raises(ValueError):
        WorkflowTaskCreateV1(order=-1)


def test_workflow_task_create():
    # Successful creation
    t = WorkflowTaskCreateV1(order=1)
    debug(t)
    # Invalid arguments
    with pytest.raises(ValidationError):
        WorkflowTaskCreateV1(order=-1)
    with pytest.raises(ValidationError):
        WorkflowTaskCreateV1(order=None)


def test_workflow_task_update():
    # Successful creation
    t = WorkflowTaskUpdateV1(meta=dict(something="else"))
    # Forbidden key-value update
    with pytest.raises(ValidationError):
        t = WorkflowTaskUpdateV1(meta=dict(parallelization_level="new"))
    debug(t)


def test_workflow_create():
    w = WorkflowCreateV1(name="workflow")
    debug(w)


def test_workflow_import():
    # Successful creation
    t = TaskImportV1(name="name", source="source")
    wft = WorkflowTaskImportV1(task=t)
    w = WorkflowImportV1(name="workflow", task_list=[wft])
    debug(w)
    # Empty-string argument
    with pytest.raises(ValidationError):
        WorkflowImportV1(name=" ", task_list=[wft])


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
        is_v2_compatible=False,
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


def test_workflow_update():
    WorkflowUpdateV1(name="workflow", reordered_workflowtask_ids=[0, 1, 3, 2])
    WorkflowUpdateV1(name="workflow")
    WorkflowUpdateV1(reordered_workflowtask_ids=[0, 1, 3, 2])
    with pytest.raises(ValidationError):
        WorkflowUpdateV1(name="workflow", reordered_workflowtask_ids=[1, 3, 1])
    with pytest.raises(ValidationError):
        WorkflowUpdateV1(
            name="workflow", reordered_workflowtask_ids=[1, 3, -1]
        )
