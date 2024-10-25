import datetime

import pytest
from devtools import debug
from sqlalchemy.exc import IntegrityError
from sqlmodel import select

from fractal_server.app.models.v1 import ApplyWorkflow
from fractal_server.app.models.v1 import Dataset
from fractal_server.app.models.v1 import Project
from fractal_server.app.models.v1 import Resource
from fractal_server.app.models.v1 import State
from fractal_server.app.models.v1 import Task
from fractal_server.app.models.v1 import Workflow
from fractal_server.app.models.v1 import WorkflowTask
from fractal_server.app.routes.api.v1._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


async def test_projects(db):
    p1 = Project(name="project", read_only=True)
    p2 = Project(name="project")
    assert p1.timestamp_created is not None
    assert p2.timestamp_created is not None
    db.add(p1)
    db.add(p2)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(Project))
    project_list = project_query.scalars().all()

    assert len(project_list) == 2
    # test defaults
    for project in project_list:
        assert project.user_list == []
        # delete
        await db.delete(project)

    project_query = await db.execute(select(Project))
    assert project_query.scalars().one_or_none() is None


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
    db.expunge_all()

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
    db.expunge_all()

    task_query = await db.execute(select(Task))
    task_list = task_query.scalars().all()
    assert len(task_list) == 2

    for task in task_list:
        # delete
        await db.delete(task)

    task_query = await db.execute(select(Task))
    assert task_query.scalars().one_or_none() is None


async def test_project_and_workflows(db):
    project = Project(name="project")
    # using `.project` relationship
    workflow1 = Workflow(name="workflow1", project=project)
    db.add(project)
    db.add(workflow1)
    await db.commit()
    db.expunge_all()

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

    # test relationships
    assert db_workflow1.project_id == db_project.id
    assert db_workflow1.project.model_dump() == db_project.model_dump()
    # test defaults
    assert db_workflow1.task_list == []

    # using `.project_id` attribute
    workflow2 = Workflow(name="workflow2", project_id=db_project.id)
    db.add(workflow2)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()

    workflow_query = await db.execute(select(Workflow))
    db_workflow1, db_workflow2 = workflow_query.scalars().all()

    # test relationships
    assert db_workflow1.name == workflow1.name
    assert db_workflow2.name == workflow2.name
    assert db_workflow2.project_id == db_project.id
    assert db_workflow2.project.model_dump() == db_project.model_dump()

    # delete just one workflow
    await db.delete(db_workflow2)

    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    assert db_workflow.name == workflow1.name

    # delete the project
    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()
    await db.delete(db_project)

    DB_ENGINE = Inject(get_settings).DB_ENGINE
    if DB_ENGINE == "postgres-psycopg":
        with pytest.raises(IntegrityError):
            # Workflow.project_id violates fk-contraint in Postgres
            await db.commit()
    else:
        # SQLite does not handle fk-constraints well
        await db.commit()
        db.expunge_all()

        project_query = await db.execute(select(Project))
        db_project = project_query.scalars().one_or_none()
        assert db_project is None

        workflow_query = await db.execute(select(Workflow))
        db_workflow = workflow_query.scalars().one_or_none()
        assert db_workflow is not None  # no cascade
        assert db_workflow.project_id is not None  # fk is not null
        assert db_workflow.project is None  # relationship is null


async def test_workflows_tasks_and_workflowtasks(db):

    # DB does not accept totally empty WorkflowTasks
    with pytest.raises(IntegrityError):
        empty_workflow = WorkflowTask()
        db.add(empty_workflow)
        await db.commit()
    await db.rollback()

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
    db.expunge_all()

    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    assert db_workflow.task_list == []
    task_query = await db.execute(select(Task))
    task_list = task_query.scalars().all()
    assert len(task_list) == 3

    for task in task_list:
        db.add(WorkflowTask(workflow_id=db_workflow.id, task_id=task.id))
    await db.commit()
    db.expunge_all()

    workflowtask_query = await db.execute(select(WorkflowTask))
    db_workflowtask_list = workflowtask_query.scalars().all()
    assert len(db_workflowtask_list) == 3
    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    assert len(db_workflow.task_list) == 3
    for i, task in enumerate(db_workflow.task_list):
        assert task.order == i

    task4 = Task(**tasks_common_args, source="source4")
    db.add(task4)
    await db.commit()
    await _workflow_insert_task(
        workflow_id=db_workflow.id,
        task_id=task4.id,
        db=db,
        order=1,
        meta={"meta": "test"},
    )
    db.expunge_all()

    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    assert len(db_workflow.task_list) == 4
    assert db_workflow.task_list[1].meta == {"meta": "test"}

    # test cascade (Workflow deletion removes all related WorkflowTasks)
    await db.delete(db_workflow)
    workflowtask_query = await db.execute(select(WorkflowTask))
    db_workflowtask = workflowtask_query.scalars().one_or_none()
    assert db_workflowtask is None


async def test_project_and_datasets(db):
    project = Project(name="project")
    # using `.project` relationship
    dataset1 = Dataset(name="dataset1", project=project)
    db.add(project)
    db.add(dataset1)
    await db.commit()
    db.expunge_all()

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

    # test relationships
    assert db_dataset1.project_id == db_project.id
    assert db_dataset1.project.model_dump() == db_project.model_dump()
    # test defaults
    assert db_dataset1.type is None
    assert db_dataset1.read_only is False
    assert db_dataset1.resource_list == []
    assert db_dataset1.meta == {}
    assert db_dataset1.history == []

    # using `.project_id` attribute
    dataset2 = Dataset(name="dataset2", project_id=db_project.id)
    db.add(dataset2)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()

    dataset_query = await db.execute(select(Dataset))
    db_dataset1, db_dataset2 = dataset_query.scalars().all()

    # test relationships
    assert db_dataset1.name == dataset1.name
    assert db_dataset2.name == dataset2.name
    assert db_dataset2.project_id == db_project.id
    assert db_dataset2.project.model_dump() == db_project.model_dump()

    # delete just one dataset
    await db.delete(db_dataset2)

    dataset_query = await db.execute(select(Dataset))
    db_dataset = dataset_query.scalars().one()
    assert db_dataset.name == dataset1.name

    # delete the project
    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()
    await db.delete(db_project)

    DB_ENGINE = Inject(get_settings).DB_ENGINE
    if DB_ENGINE == "postgres-psycopg":
        with pytest.raises(IntegrityError):
            # Dataset.project_id violates fk-contraint in Postgres
            await db.commit()
    else:
        # SQLite does not handle fk-constraints well
        await db.commit()
        db.expunge_all()

        project_query = await db.execute(select(Project))
        db_project = project_query.scalars().one_or_none()
        assert db_project is None

        dataset_query = await db.execute(select(Dataset))
        db_dataset = dataset_query.scalars().one_or_none()
        assert db_dataset is not None  # no cascade
        assert db_dataset.project_id is not None  # fk is not null
        assert db_dataset.project is None  # relationship is null


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
    db.expunge_all()

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
    db.expunge_all()

    dataset_query = await db.execute(select(Dataset))
    db_dataset = dataset_query.scalars().one()
    # assert Dataset.resource_list is ordered by Resource.id
    assert [rsc.id for rsc in db_dataset.resource_list] == [
        resource2.id,  # 20,
        resource1.id,  # 100,
    ]

    resource_query = await db.execute(select(Resource))
    db_resource1, db_resource2 = resource_query.scalars().all()
    await db.delete(db_resource2)
    await db.commit()
    db.expunge_all()

    resource_query = await db.execute(select(Resource))
    db_resource = resource_query.scalars().one()
    dataset_query = await db.execute(select(Dataset))
    db_dataset = dataset_query.scalars().one()
    assert db_dataset.resource_list == [db_resource]

    await db.delete(db_dataset)
    await db.commit()
    db.expunge_all()
    # test cascade
    resource_query = await db.execute(select(Resource))
    db_resource = resource_query.scalars().one_or_none()
    assert db_resource is None


async def test_jobs(db):
    required_args = dict(
        user_email="test@fractal.xy",
        project_dump={},
        input_dataset_dump={},
        output_dataset_dump={},
        workflow_dump={},
        first_task_index=0,
        last_task_index=0,
    )
    # test that every arg of default_args is required:
    # fails if one arg is removed, succeed if all args are there
    for arg in required_args:
        with pytest.raises(IntegrityError):
            job = ApplyWorkflow(
                **{k: v for k, v in required_args.items() if k != arg}
            )
            db.add(job)
            await db.commit()
        await db.rollback()
    job = ApplyWorkflow(**required_args)
    db.add(job)
    await db.commit()
    db.expunge_all()
    job_query = await db.execute(select(ApplyWorkflow))
    db_job = job_query.scalars().one()
    # delete
    await db.delete(db_job)
    job_query = await db.execute(select(WorkflowTask))
    assert job_query.scalars().one_or_none() is None

    project = Project(name="project")
    input_dataset = Dataset(name="input dataset", project=project)
    output_dataset = Dataset(name="output dataset", project=project)
    workflow = Workflow(name="workflow", project=project)
    db.add(project)
    db.add(input_dataset)
    db.add(output_dataset)
    db.add(workflow)
    await db.commit()
    db.expunge_all()

    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()
    dataset_query = await db.execute(select(Dataset))
    db_input_dataset, db_output_dataset = dataset_query.scalars().all()
    assert db_input_dataset.name == "input dataset"
    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()

    N_JOBS = 3
    for _ in range(N_JOBS):
        job = ApplyWorkflow(
            **required_args,
            project_id=db_project.id,
            workflow_id=db_workflow.id,
            input_dataset_id=db_input_dataset.id,
            output_dataset_id=db_output_dataset.id,
        )
        db.add(job)
    await db.commit()
    db.expunge_all()

    # test relationships
    dataset_query = await db.execute(select(Dataset))
    db_input_dataset, db_output_dataset = dataset_query.scalars().all()
    assert db_input_dataset.name == "input dataset"
    assert db_output_dataset.name == "output dataset"
    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    job_query = await db.execute(select(ApplyWorkflow))
    db_jobs = job_query.scalars().all()

    assert len(db_jobs) == N_JOBS
    for job in db_jobs:
        assert job.workflow_id is not None
        assert job.input_dataset_id is not None
        assert job.output_dataset_id is not None
        assert job.project_id is not None

    DB_ENGINE = Inject(get_settings).DB_ENGINE

    # Test deletion of related objects.
    # NOTE: Since there are no cascade relationships between Dataset/Workflow
    # and ApplyWorkflow, foreign-keys would remain non-null even after delete
    # operations, leading to integrity errors.
    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    input_dataset_query = await db.execute(
        select(Dataset).where(Dataset.name == input_dataset.name)
    )
    db_input_dataset = input_dataset_query.scalars().one()
    output_dataset_query = await db.execute(
        select(Dataset).where(Dataset.name == output_dataset.name)
    )
    db_output_dataset = output_dataset_query.scalars().one()

    # Failed deletions
    for db_object in (db_workflow, db_input_dataset, db_output_dataset):
        if DB_ENGINE == "postgres-psycopg":
            with pytest.raises(IntegrityError):
                await db.delete(db_object)
                await db.commit()
            await db.rollback()
        else:
            # Skip this block when using sqlite, since its implementation of
            # foreign-key constraints may actually allow these delete
            # operations.
            pass

    # Successful deletions (after making foreign keys null)
    job_query = await db.execute(select(ApplyWorkflow))
    db_jobs = job_query.scalars().all()
    assert len(db_jobs) == N_JOBS
    for job in db_jobs:
        job.workflow_id = None
        job.input_dataset_id = None
        job.output_dataset_id = None
        await db.merge(job)
    await db.commit()
    workflow_query = await db.execute(select(Workflow))
    db_workflow = workflow_query.scalars().one()
    input_dataset_query = await db.execute(
        select(Dataset).where(Dataset.name == input_dataset.name)
    )
    db_input_dataset = input_dataset_query.scalars().one()
    output_dataset_query = await db.execute(
        select(Dataset).where(Dataset.name == output_dataset.name)
    )
    db_output_dataset = output_dataset_query.scalars().one()
    await db.delete(db_workflow)
    await db.delete(db_input_dataset)
    await db.delete(db_output_dataset)
    await db.commit()

    # Failed project deletion
    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()
    if DB_ENGINE == "postgres-psycopg":
        with pytest.raises(IntegrityError):
            await db.delete(db_project)
            await db.commit()
        await db.rollback()
    else:
        # Skip this block when using sqlite, since its implementation of
        # foreign-key constraints may actually allow these delete operations.
        pass

    # Successful project deletion (after making foreign keys null)
    job_query = await db.execute(select(ApplyWorkflow))
    db_jobs = job_query.scalars().all()
    assert len(db_jobs) == N_JOBS
    for job in db_jobs:
        job.project_id = None
        await db.merge(job)
    await db.commit()
    project_query = await db.execute(select(Project))
    db_project = project_query.scalars().one()
    await db.delete(db_project)
    await db.commit()


async def test_project_name_not_unique(MockCurrentUser, db, project_factory):
    """
    GIVEN the fractal_server database
    WHEN I create two projects with the same name and same user
    THEN no exception is raised
    """
    PROJ_NAME = "project name"
    async with MockCurrentUser() as user:
        p0 = await project_factory(user, name=PROJ_NAME)
        p1 = await project_factory(user, name=PROJ_NAME)

    stm = select(Project).where(Project.name == PROJ_NAME)
    res = await db.execute(stm)
    project_list = res.scalars().all()
    assert len(project_list) == 2
    assert p0.model_dump() in [p.model_dump() for p in project_list]
    assert p1.model_dump() in [p.model_dump() for p in project_list]


async def test_task_workflow_association(
    project_factory, MockCurrentUser, task_factory, db
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        t0 = await task_factory(source="source0")
        t1 = await task_factory(source="source1")

        wf = Workflow(name="my wfl", project_id=project.id)
        args = dict(arg="test arg")

        db.add(wf)
        await db.commit()
        await db.refresh(wf)

        # insert_task fail if Workflow with workflow_id is not found
        with pytest.raises(ValueError):
            await _workflow_insert_task(
                workflow_id=12345, task_id=t0.id, db=db, args=args
            )
        # insert_task fail if Task with task_id is not found
        with pytest.raises(ValueError):
            await _workflow_insert_task(
                workflow_id=wf.id, task_id=12345, db=db, args=args
            )

        await _workflow_insert_task(
            workflow_id=wf.id, task_id=t0.id, db=db, args=args
        )

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
        await _workflow_insert_task(
            workflow_id=wf.id, task_id=t1.id, db=db, order=0
        )
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
    async with MockCurrentUser() as user:
        # Create a task with a valid args_schema
        args_schema = {
            "title": "_Arguments",
            "type": "object",
            "properties": {
                "arg_no_default": {
                    "title": "Arg No Default",
                    "type": "integer",
                },
                "arg_default_one": {
                    "title": "Arg Default One",
                    "default": "one",
                    "type": "string",
                },
                "arg_default_none": {
                    "title": "Arg Default None",
                    "type": "string",
                },
                "innerA": {"$ref": "#/definitions/_InnerArgument"},
                "innerB": {
                    "title": "Innerb",
                    "default": {"x": 11, "y": 2},
                    "allOf": [{"$ref": "#/definitions/_InnerArgument"}],
                },
            },
            "required": ["arg_no_default", "innerA"],
            "definitions": {
                "_InnerArgument": {
                    "title": "_InnerArgument",
                    "type": "object",
                    "properties": {
                        "x": {"title": "X", "type": "integer"},
                        "y": {"title": "Y", "default": 2, "type": "integer"},
                    },
                    "required": ["x"],
                }
            },
        }
        t0 = await task_factory(source="source0", args_schema=args_schema)

        # Create project and workflow
        project = await project_factory(user)
        wf = Workflow(name="my wfl", project_id=project.id)
        db.add(wf)
        await db.commit()
        await db.refresh(wf)

        # Insert task into workflow, without/with additional args
        db.add(wf)
        await db.commit()
        await db.refresh(wf)
        await _workflow_insert_task(workflow_id=wf.id, task_id=t0.id, db=db)
        await _workflow_insert_task(
            workflow_id=wf.id,
            task_id=t0.id,
            db=db,
            args=dict(arg_default_one="two"),
        )
        await _workflow_insert_task(
            workflow_id=wf.id,
            task_id=t0.id,
            db=db,
            args=dict(arg_default_none="three"),
        )

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

        db.add(wf)
        await db.commit()
        await db.refresh(wf)
        # Insert task with invalid args_schema into workflow
        await _workflow_insert_task(workflow_id=wf.id, task_id=t1.id, db=db)
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
    async with MockCurrentUser() as user:
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

        await _workflow_insert_task(workflow_id=wf_id, task_id=t0.id, db=db)
        await _workflow_insert_task(workflow_id=wf_id, task_id=t1.id, db=db)

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
    args_schema = {
        "title": "_Arguments",
        "type": "object",
        "properties": {
            "a": {
                "title": "A",
                "type": "integer",
            },
            "b": {
                "title": "B",
                "default": "one",
                "type": "string",
            },
            "c": {
                "title": "C",
                "type": "string",
            },
            "d": {
                "title": "D",
                "default": [1, 2, 3],
                "type": "array",
                "items": {
                    "type": "integer",
                },
            },
        },
        "required": ["a"],
    }
    expected_default_args = {"b": "one", "d": [1, 2, 3]}

    async with MockCurrentUser():
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
    async with MockCurrentUser():
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
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        t0 = await task_factory(source="source0", meta=None)
        wf = Workflow(name="my wfl", project_id=project.id)
        db.add(wf)
        await db.commit()
        await db.refresh(wf)
        args = dict(arg="test arg")
        db.add(wf)
        await db.commit()
        await db.refresh(wf)
        await _workflow_insert_task(
            workflow_id=wf.id, task_id=t0.id, db=db, args=args
        )


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


async def test_timestamp(db):
    """
    SQLite encodes datetime objects as strings; therefore when extracting a
    timestamp from the db, it is not timezone-aware by default.
    Regarding Postgres:
    https://www.psycopg.org/psycopg3/docs/basic/adapt.html#date-time-types-adaptation
    This test asserts this behaviour.
    """
    p = Project(name="project")
    assert isinstance(p.timestamp_created, datetime.datetime)
    assert p.timestamp_created.tzinfo == datetime.timezone.utc
    assert p.timestamp_created.tzname() == "UTC"

    db.add(p)
    await db.commit()
    db.expunge_all()

    query = await db.execute(select(Project))
    project = query.scalars().one()

    assert isinstance(project.timestamp_created, datetime.datetime)

    DB_ENGINE = Inject(get_settings).DB_ENGINE
    if DB_ENGINE == "sqlite":
        assert project.timestamp_created.tzinfo is None
        assert project.timestamp_created.tzname() is None
    elif DB_ENGINE == "postgres-psycopg":
        assert project.timestamp_created.tzinfo is not None
        assert (
            project.timestamp_created.astimezone(tz=datetime.timezone.utc)
            == p.timestamp_created
        )
