import pytest
from devtools import debug
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from fractal_server.app.models import Dataset
from fractal_server.app.models import Project
from fractal_server.app.models import Resource
from fractal_server.app.models import State
from fractal_server.app.models import Task
from fractal_server.app.models import Workflow
from fractal_server.app.models import WorkflowTask


async def test_projects(db):

    p1 = Project(name="project", read_only=True)
    p2 = Project(name="project")
    db.add(p1)
    db.add(p2)
    await db.commit()
    await db.close()

    project_query = await db.execute(select(Project))
    project_list = project_query.scalars().all()

    assert len(project_list) == 2
    # test defaults
    for project in project_list:
        assert project.user_list == []
        assert project.dataset_list == []
        assert project.workflow_list == []
        assert project.job_list == []


async def test_tasks(db):
    args = dict(
        name="name",
        command="command",
        source="source",
        input_type="input_type",
        output_type="output_type",
    )
    task1 = Task(**args)
    db.add(task1)
    await db.commit()
    await db.close()

    task_query = await db.execute(select(Task))
    db_task = task_query.scalars().one()

    for arg, value in args.items():
        assert getattr(db_task, arg) == value
    # test_defaults
    assert db_task.meta == {}
    assert db_task.owner is None
    assert db_task.version is None
    assert db_task.args_schema is None
    assert db_task.args_schema_version is None
    assert db_task.docs_info is None
    assert db_task.docs_link is None

    # `Task.source` has unique constraint
    broken_task = Task(**args)  # == task1
    with pytest.raises(IntegrityError):
        db.add(broken_task)
        await db.commit()
    await db.rollback()
    # change `.source`
    args["source"] = "different source"
    task2 = Task(**args)
    db.add(task2)
    await db.commit()
    await db.close()

    task_query = await db.execute(select(Task))
    task_list = task_query.scalars().all()
    assert len(task_list) == 2


async def test_project_and_workflows(db):

    project = Project(name="project")
    # using `.project` relationship
    workflow1 = Workflow(name="workflow1", project=project)
    db.add(project)
    db.add(workflow1)
    await db.commit()
    await db.close()

    with pytest.raises(IntegrityError):
        # missing relatioship with project
        broken_workflow = Workflow(name="broken")
        db.add(broken_workflow)
        await db.commit()
    await db.rollback()

    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()
    workflow_query = await db.execute(select(Workflow))
    db_workflow1 = workflow_query.scalars().one()

    assert len(db_project.workflow_list) == 1
    # test relationships
    assert db_project.workflow_list[0] == db_workflow1
    assert db_workflow1.project_id == db_project.id
    assert db_workflow1.project == db_project
    # test defaults
    assert db_workflow1.task_list == []
    assert db_workflow1.job_list == []

    # using `.project_id` attribute
    workflow2 = Workflow(name="workflow2", project_id=db_project.id)
    db.add(workflow2)
    await db.commit()
    await db.close()

    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()

    workflow_query = await db.execute(select(Workflow))
    db_workflow1, db_workflow2 = workflow_query.scalars().all()

    assert len(db_project.workflow_list) == 2
    # test relationships
    assert db_project.workflow_list == [db_workflow1, db_workflow2]
    assert db_workflow1.name == workflow1.name
    assert db_workflow2.name == workflow2.name
    assert db_workflow2.project_id == db_project.id
    assert db_workflow2.project == db_project


async def test_workflows_tasks_and_workflowtasks(db):

    # DB accepts totally empty WorkflowTasks
    db.add(WorkflowTask())
    await db.commit()
    await db.close()
    wftask_query = await db.execute(select(WorkflowTask))
    db_wftask = wftask_query.scalars().one()
    # test defaults
    assert db_wftask.task_id is None
    assert db_wftask.meta is None
    assert db_wftask.args is None
    assert db_wftask.workflow_id is None
    assert db_wftask.order is None
    assert db_wftask.task is None
    # delete
    await db.delete(db_wftask)
    wftask_query = await db.execute(select(WorkflowTask))
    assert wftask_query.scalars().one_or_none() is None

    project = Project(name="project")
    workflow = Workflow(name="workflow", project=project)
    tasks_common_args = dict(
        name="name",
        command="command",
        input_type="input_type",
        output_type="output_type",
    )
    task1 = Task(**tasks_common_args, source="source1")
    task2 = Task(**tasks_common_args, source="source2")
    task3 = Task(**tasks_common_args, source="source3")
    db.add(project)
    db.add(workflow)
    db.add(task1)
    db.add(task2)
    db.add(task3)
    await db.commit()
    await db.close()

    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    assert db_workflow.task_list == []
    task_query = await db.execute(select(Task))
    task_list = task_query.scalars().all()
    assert len(task_list) == 3

    for task in task_list:
        db.add(WorkflowTask(workflow_id=db_workflow.id, task_id=task.id))
    await db.commit()
    await db.close()

    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    for i, task in enumerate(db_workflow.task_list):
        assert task.order == i


async def test_project_and_datasets(db):

    project = Project(name="project")
    # using `.project` relationship
    dataset1 = Dataset(name="dataset1", project=project)
    db.add(project)
    db.add(dataset1)
    await db.commit()
    await db.close()

    with pytest.raises(IntegrityError):
        # missing relatioship with project
        broken_dataset = Dataset(name="broken")
        db.add(broken_dataset)
        await db.commit()
    await db.rollback()

    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()
    dataset_query = await db.execute(select(Dataset))
    db_dataset1 = dataset_query.scalars().one()

    assert len(db_project.dataset_list) == 1
    # test relationships
    assert db_project.dataset_list[0] == db_dataset1
    assert db_dataset1.project_id == db_project.id
    assert db_dataset1.project == db_project
    # test defaults
    assert db_dataset1.type is None
    assert db_dataset1.read_only is False
    assert db_dataset1.list_jobs_input == []
    assert db_dataset1.list_jobs_output == []
    assert db_dataset1.resource_list == []
    assert db_dataset1.meta == {}
    assert db_dataset1.history == []

    # using `.project_id` attribute
    dataset2 = Dataset(name="dataset2", project_id=db_project.id)
    db.add(dataset2)
    await db.commit()
    await db.close()

    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()

    dataset_query = await db.execute(select(Dataset))
    db_dataset1, db_dataset2 = dataset_query.scalars().all()

    assert len(db_project.dataset_list) == 2
    # test relationships
    assert db_project.dataset_list == [db_dataset1, db_dataset2]
    assert db_dataset1.name == "dataset1"
    assert db_dataset2.name == "dataset2"
    assert db_dataset2.project_id == db_project.id
    assert db_dataset2.project == db_project


async def test_dataset_and_resources(db):

    project = Project(name="project")
    resource1 = Resource(id=100, path="/rsc1")
    # using `Dataset.resource_list`
    dataset = Dataset(
        name="dataset2", project=project, resource_list=[resource1]
    )
    db.add(project)
    db.add(dataset)
    await db.commit()
    await db.close()

    with pytest.raises(IntegrityError):
        # missing relatioship with dataset
        broken_resource = Resource(path="/broken")
        db.add(broken_resource)
        await db.commit()
    await db.rollback()

    dataset_query = await db.execute(select(Dataset))
    db_dataset = dataset_query.scalars().one()
    resource_query = await db.execute(select(Resource))
    db_resource1 = resource_query.scalars().one()

    # test relationships
    assert db_dataset.resource_list == [db_resource1]
    assert db_resource1.dataset_id == db_dataset.id

    # using `.dataset_id` attribute
    resource2 = Resource(id=20, path="/rsc2", dataset_id=db_dataset.id)
    db.add(resource2)
    await db.commit()
    await db.close()

    dataset_query = await db.execute(select(Dataset))
    db_dataset = dataset_query.scalars().one()
    # assert Dataset.resource_list is ordered by Resource.id
    assert [rsc.id for rsc in db_dataset.resource_list] == [
        resource2.id,  # 20,
        resource1.id,  # 100,
    ]


async def test_project_name_not_unique(MockCurrentUser, db, project_factory):
    """
    GIVEN the fractal_server database
    WHEN I create two projects with the same name and same user
    THEN no exception is raised
    """
    PROJ_NAME = "project name"
    async with MockCurrentUser(persist=True) as user:
        p0 = await project_factory(user, name=PROJ_NAME)
        p1 = await project_factory(user, name=PROJ_NAME)

    stm = select(Project).where(Project.name == PROJ_NAME)
    res = await db.execute(stm)
    project_list = res.scalars().all()
    assert len(project_list) == 2
    assert p0 in project_list
    assert p1 in project_list


async def test_task_workflow_association(
    db, project_factory, MockCurrentUser, task_factory
):
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        t0 = await task_factory(source="source0")
        t1 = await task_factory(source="source1")

        wf = Workflow(name="my wfl", project_id=project.id)
        args = dict(arg="test arg")
        await wf.insert_task(t0.id, db=db, args=args)

        db.add(wf)
        await db.commit()
        await db.refresh(wf)

        debug(wf)
        assert wf.task_list[0].args == args
        # check workflow
        assert len(wf.task_list) == 1
        assert wf.task_list[0].id == t0.id
        # check association table
        stm = (
            select(WorkflowTask)
            .where(WorkflowTask.workflow_id == wf.id)
            .where(WorkflowTask.task_id == t0.id)
        )
        res = await db.execute(stm)
        link = res.scalars().one()
        debug(link)
        assert link.task_id == t0.id

        # Insert at position 0
        await wf.insert_task(t1.id, order=0, db=db)
        db.add(wf)
        await db.commit()
        await db.refresh(wf)

        stm = (
            select(WorkflowTask)
            .where(WorkflowTask.workflow_id == wf.id)
            .where(WorkflowTask.task_id == t1.id)
        )
        res = await db.execute(stm)
        link = res.scalars().one()
        debug(link)
        assert link.order == 0
        assert link.task_id == t1.id


async def test_workflow_insert_task_with_args_schema(
    db, project_factory, MockCurrentUser, task_factory
):
    """
    GIVEN a Workflow, and a Task with valid args_schema (including defaults)
    WHEN the Task is inserted into the Workflow
    THEN the WorkflowTask.args attribute is set correctly
    """
    from pydantic import BaseModel
    from typing import Optional

    async with MockCurrentUser(persist=True) as user:

        # Prepare models to generate a valid JSON Schema
        class _InnerArgument(BaseModel):
            x: int
            y: int = 2

        class _Arguments(BaseModel):
            arg_no_default: int
            arg_default_one: str = "one"
            arg_default_none: Optional[str] = None
            innerA: _InnerArgument
            innerB: _InnerArgument = _InnerArgument(x=11)

        # Create a task with a valid args_schema
        args_schema = _Arguments.schema()
        debug(args_schema)
        t0 = await task_factory(source="source0", args_schema=args_schema)

        # Create project and workflow
        project = await project_factory(user)
        wf = Workflow(name="my wfl", project_id=project.id)

        # Insert task into workflow, without/with additional args
        await wf.insert_task(t0.id, db=db)
        await wf.insert_task(t0.id, db=db, args=dict(arg_default_one="two"))
        await wf.insert_task(t0.id, db=db, args=dict(arg_default_none="three"))
        db.add(wf)
        await db.commit()
        await db.refresh(wf)

        # Verify taht args were set correctly
        wftask1, wftask2, wftask3 = wf.task_list[:]
        debug(wftask1.args)
        assert wftask1.args == dict(
            arg_default_one="one",
            innerB=dict(x=11, y=2),
        )
        debug(wftask2.args)
        assert wftask2.args == dict(
            arg_default_one="two",
            innerB=dict(x=11, y=2),
        )
        debug(wftask3.args)
        assert wftask3.args == dict(
            arg_default_one="one",
            arg_default_none="three",
            innerB=dict(x=11, y=2),
        )

        # Create a task with an invalid args_schema
        invalid_args_schema = args_schema.copy()
        invalid_args_schema["xxx"] = invalid_args_schema.pop("properties")
        t1 = await task_factory(
            source="source1", args_schema=invalid_args_schema
        )

        # Insert task with invalid args_schema into workflow
        await wf.insert_task(t1.id, db=db)
        db.add(wf)
        await db.commit()
        await db.refresh(wf)
        wftask4 = wf.task_list[-1]
        debug(wftask4)
        assert wftask4.args is None


async def test_cascade_delete_workflow(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Workflow
    WHEN the Workflow is deleted
    THEN all the related WorkflowTask are deleted
    """
    async with MockCurrentUser(persist=True) as user:

        project = await project_factory(user=user)

        workflow = Workflow(
            name="My Workflow",
            project_id=project.id,
        )

        db.add(workflow)
        await db.commit()
        await db.refresh(workflow)
        wf_id = workflow.id

        t0 = await task_factory(source="source0")
        t1 = await task_factory(source="source1")

        await workflow.insert_task(t0.id, db=db)
        await workflow.insert_task(t1.id, db=db)

        await db.refresh(workflow)

        before_delete_wft_ids = [_wft.id for _wft in workflow.task_list]

        await db.delete(workflow)
        await db.commit()

        del_workflow = await db.get(Workflow, wf_id)
        assert del_workflow is None

        after_delete_wft_ids = (
            (await db.execute(select(WorkflowTask.id))).scalars().all()
        )

        debug(set(after_delete_wft_ids), set(before_delete_wft_ids))
        assert not set(after_delete_wft_ids).intersection(
            set(before_delete_wft_ids)
        )


async def test_state_table(db):
    """
    GIVEN the State table
    WHEN queried
    THEN one can CRUD items
    """
    payload = dict(a=1, b=2, c="tre")
    db.add(State(data=payload))
    await db.commit()

    res = await db.execute(select(State))
    state_list = res.scalars().all()
    debug(state_list)
    assert len(state_list) == 1
    assert state_list[0].data == payload


async def test_task_default_args_from_args_schema(
    client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Task with args_schema
    THEN the default_args_from_args_schema property works as expected
    """

    from pydantic import BaseModel
    from typing import Optional

    # Prepare models to generate a valid JSON Schema
    class _Arguments(BaseModel):
        a: int
        b: str = "one"
        c: Optional[str] = None
        d: list[int] = [1, 2, 3]

    args_schema = _Arguments.schema()
    debug(args_schema)
    expected_default_args = {"b": "one", "d": [1, 2, 3]}

    async with MockCurrentUser(persist=True):
        task = await task_factory(
            name="task with schema",
            source="source0",
            command="cmd",
            input_type="Any",
            output_type="Any",
            args_schema_version="something",
            args_schema=args_schema,
        )
        debug(task.default_args_from_args_schema)
        assert task.default_args_from_args_schema == expected_default_args

    invalid_args_schema = {"something": "else"}
    async with MockCurrentUser(persist=True):
        task = await task_factory(
            name="task with schema",
            source="source1",
            command="cmd",
            input_type="Any",
            output_type="Any",
            args_schema_version="something",
            args_schema=invalid_args_schema,
        )
        debug(task.default_args_from_args_schema)
        assert task.default_args_from_args_schema == {}


async def test_insert_task_with_meta_none(
    db, project_factory, MockCurrentUser, task_factory
):
    """
    Test insertion of a task which has `task.meta=None`, see
    https://github.com/fractal-analytics-platform/fractal-server/issues/770
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        t0 = await task_factory(source="source0", meta=None)
        wf = Workflow(name="my wfl", project_id=project.id)
        args = dict(arg="test arg")
        await wf.insert_task(t0.id, db=db, args=args)


async def test_project_relationships(db):
    """
    Test Project/Workflow and Project/Dataset relationships
    """
    # Establish relationships via foreign key
    proj = Project(name="proj", id=1)
    wf1 = Workflow(name="wf1", project_id=1, id=11)
    ds1 = Dataset(name="ds1", project_id=1, id=111)
    db.add(proj)
    db.add(wf1)
    db.add(ds1)
    await db.commit()

    # Test relationships
    await db.refresh(wf1)
    await db.refresh(ds1)
    assert wf1.project.name == proj.name
    assert ds1.project.name == proj.name

    # Establish relationships via {Dataset,Workflow}.project
    ds3 = Dataset(name="ds3", project=proj)
    wf3 = Workflow(name="wf3", project=proj)
    db.add(ds3)
    db.add(wf3)
    await db.commit()

    # Test relationships
    await db.refresh(wf3)
    await db.refresh(ds3)
    assert wf3.project.name == proj.name
    assert ds3.project.name == proj.name
