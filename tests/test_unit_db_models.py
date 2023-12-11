from devtools import debug
from sqlmodel import select

from fractal_server.app.models import Project
from fractal_server.app.models import State
from fractal_server.app.models.workflow import Workflow
from fractal_server.app.models.workflow import WorkflowTask


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


async def test_cascade_delete_project(
    db, client, MockCurrentUser, project_factory, task_factory
):
    """
    GIVEN a Project
    WHEN the Project is deleted
    THEN all the related Workflows are deleted
    """

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user=user)
        project_id = project.id

        workflow1 = Workflow(
            name="My first Workflow",
            project_id=project.id,
        )
        workflow2 = Workflow(
            name="My second Workflow",
            project_id=project.id,
        )
        db.add(workflow1)
        db.add(workflow2)
        await db.commit()

        await db.refresh(project)
        await db.refresh(workflow1)
        await db.refresh(workflow2)

        before_delete_wf_ids = [wf.id for wf in project.workflow_list]

        await db.delete(project)
        await db.commit()

        assert not await db.get(Project, project_id)

        after_delete_wf_ids = (
            (await db.execute(select(Workflow.id))).scalars().all()
        )

        debug(set(before_delete_wf_ids), set(after_delete_wf_ids))
        assert not set(after_delete_wf_ids).intersection(
            set(before_delete_wf_ids)
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


async def test_sorted_relationships(
    db,
    MockCurrentUser,
    project_factory,
    workflow_factory,
    dataset_factory,
    resource_factory,
    task_factory,
    job_factory,
    tmp_path,
):

    async with MockCurrentUser(persist=True) as user:

        NAMES = ["B", "A1", "a1", "A2", "BB", "A"]
        IDS = [10, 1, 9, 2, 8, 3, 7, 4, 6, 5]

        project = await project_factory(user)

        # Project.workflow_list is sorted by Workflow.name
        for _id in IDS:
            await workflow_factory(id=_id, project_id=project.id, name=_id)
        await db.refresh(project)
        assert [wf.id for wf in project.workflow_list] == sorted(IDS)

        # Project.dataset_list is sorted by Dataset.name
        for name in NAMES:
            await dataset_factory(project_id=project.id, name=name)
        await db.refresh(project)
        assert [ds.name for ds in project.dataset_list] == sorted(
            NAMES, key=str.casefold
        )

        # Dataset.resource_list is sorted by Resource.id
        dataset = project.dataset_list[0]
        for resource_id in IDS:
            await resource_factory(dataset, id=resource_id)
        await db.refresh(dataset)
        assert [res.id for res in dataset.resource_list] == sorted(IDS)

        # Workflow.task_list is sorted by Task.order
        workflow = project.workflow_list[0]
        for i in range(len(IDS)):
            task = await task_factory(
                input_type=f"type{i}",
                output_type=f"type{i+1}",
                source=f"test:{i}to{i+1}",
            )
            await workflow.insert_task(task.id, db=db, order=IDS[i])

        await db.refresh(workflow)
        assert [task.id for task in workflow.task_list] != list(
            range(len(IDS))
        )
        assert [task.order for task in workflow.task_list] == list(
            range(len(IDS))  # this is not `sorted(IDS)` because order is 0..N
        )

        # Project.job_list, Workflow.job_list and Dataset.list_jobs_{in,out}put
        # are sorted by ApplyWorkflow.id
        for i in IDS:
            await job_factory(
                id=i,
                project_id=project.id,
                workflow_id=workflow.id,
                input_dataset_id=dataset.id,
                output_dataset_id=dataset.id,
                working_dir=(tmp_path / "some_working_dir").as_posix(),
            )
        await db.refresh(project)
        await db.refresh(workflow)
        await db.refresh(dataset)

        assert [job.id for job in project.job_list] == sorted(IDS)
        assert [job.id for job in workflow.job_list] == sorted(IDS)
        assert [job.id for job in dataset.list_jobs_input] == sorted(IDS)
        assert [job.id for job in dataset.list_jobs_output] == sorted(IDS)
