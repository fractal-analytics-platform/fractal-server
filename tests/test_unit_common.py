from datetime import datetime

import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.common.schemas import _StateBase
from fractal_server.common.schemas import ApplyWorkflowCreate
from fractal_server.common.schemas import ApplyWorkflowRead
from fractal_server.common.schemas import DatasetCreate
from fractal_server.common.schemas import DatasetRead
from fractal_server.common.schemas import DatasetUpdate
from fractal_server.common.schemas import ManifestV1
from fractal_server.common.schemas import ProjectCreate
from fractal_server.common.schemas import ResourceCreate
from fractal_server.common.schemas import ResourceRead
from fractal_server.common.schemas import StateRead
from fractal_server.common.schemas import TaskCollectPip
from fractal_server.common.schemas import TaskCreate
from fractal_server.common.schemas import TaskImport
from fractal_server.common.schemas import TaskManifestV1
from fractal_server.common.schemas import TaskRead
from fractal_server.common.schemas import TaskUpdate
from fractal_server.common.schemas import UserCreate
from fractal_server.common.schemas import WorkflowCreate
from fractal_server.common.schemas import WorkflowImport
from fractal_server.common.schemas import WorkflowRead
from fractal_server.common.schemas import WorkflowTaskCreate
from fractal_server.common.schemas import WorkflowTaskImport
from fractal_server.common.schemas import WorkflowTaskRead
from fractal_server.common.schemas import WorkflowTaskUpdate
from fractal_server.common.schemas import WorkflowUpdate


def test_apply_workflow_create():
    # Valid ApplyWorkflowCreate instance
    valid_args = dict(worker_init="WORKER INIT")
    job = ApplyWorkflowCreate(**valid_args)
    debug(job)

    with pytest.raises(ValueError) as e:
        job = ApplyWorkflowCreate(first_task_index=-1)
    debug(e)

    with pytest.raises(ValueError) as e:
        job = ApplyWorkflowCreate(last_task_index=-1)
    debug(e)

    with pytest.raises(ValueError) as e:
        job = ApplyWorkflowCreate(first_task_index=2, last_task_index=0)
    debug(e)


def test_apply_workflow_read():
    x = ApplyWorkflowRead(
        id=1,
        project_id=1,
        workflow_id=1,
        input_dataset_id=1,
        output_dataset_id=1,
        start_timestamp="2019-12-23T23:10:11.115310Z",
        status="good",
        workflow_dump=dict(task_list=[]),
    )

    assert isinstance(x.start_timestamp, datetime)
    y = x.sanitised_dict()
    assert isinstance(y["start_timestamp"], str)


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
    p = ProjectCreate(name="my project")
    debug(p)
    # Check that whitespaces are stripped from beginning/end of string
    NAME = "some project name"
    p = ProjectCreate(name=f"  {NAME}  ")
    debug(p)
    assert p.name == NAME
    # Fail due to empty string
    with pytest.raises(ValidationError):
        ProjectCreate(name="  ")


def test_state():
    s = _StateBase(data={"some": "thing"}, timestamp=datetime.now())
    debug(s)
    debug(s.sanitised_dict())
    assert isinstance(s.sanitised_dict()["timestamp"], str)


def test_state_read():
    s = StateRead(data={"some": "thing"}, timestamp=datetime.now())
    debug(s)
    assert s.id is None

    s = StateRead(data={"some": "thing"}, timestamp=datetime.now(), id=1)
    debug(s)
    assert s.id == 1


def test_TaskCollectPip():
    # Successful creation
    c = TaskCollectPip(package="some-package")
    debug(c)
    assert c
    c = TaskCollectPip(package="/some/package.whl")
    debug(c)
    assert c
    # Failed creation
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some/package")
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="/some/package.tar.gz")
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some-package", package_extras="")
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some-package", package_extras=None)

    c = TaskCollectPip(package="some-package", pinned_package_versions={})
    with pytest.raises(ValidationError):
        c = TaskCollectPip(package="some-package", pinned_package_versions=1)


def test_task_update():
    # Successful creation, with many unset fields
    t = TaskUpdate(name="name")
    debug(t)
    assert list(t.dict(exclude_none=True).keys()) == ["name"]
    assert list(t.dict(exclude_unset=True).keys()) == ["name"]
    # Some failures
    with pytest.raises(ValidationError):
        TaskUpdate(name="task", version="")
    TaskUpdate(name="task", version=None)
    # Successful cretion, with mutliple fields set
    t = TaskUpdate(
        name="task",
        version="1.2.3",
        owner="someone",
    )
    debug(t)
    assert t.name
    assert t.version


def test_task_create():
    # Successful creation
    t = TaskCreate(
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
        TaskCreate(name="task", source="source")

    # Bad docs link
    with pytest.raises(ValidationError):
        TaskCreate(
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
    # With valid slurm_user attribute
    u = UserCreate(email="a@b.c", password="asd", slurm_user="slurm_user")
    debug(u)
    assert u.slurm_user
    # With invalid slurm_user attribute
    with pytest.raises(ValidationError):
        u = UserCreate(email="a@b.c", password="asd", slurm_user="  ")
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
