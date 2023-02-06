import json

from devtools import debug
from sqlmodel import select

from fractal_server.app.models import Task
from fractal_server.app.models import Workflow
from fractal_server.common.schemas import WorkflowCreate
from fractal_server.common.schemas import WorkflowTaskCreate


async def test_export_workflow(
    db,
    project_factory,
    MockCurrentUser,
    collect_packages,
    tmp_path,
):

    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user=user)

    # Add dummy task as a Task
    task_dummy = collect_packages[0]

    # Create a workflow with some WorkflowTasks
    wf = Workflow(name="MyWorkflow", project_id=prj.id)
    db.add(wf)
    await db.commit()
    await db.refresh(wf)
    await wf.insert_task(task_dummy.id, db=db, args=dict(message="task 0"))
    await wf.insert_task(task_dummy.id, db=db, args=dict(message="task 1"))
    await db.refresh(wf)

    debug(wf)
    wf_to_export = wf.dict_with_relationships_but_no_ids()
    debug(wf_to_export)

    assert "id" not in wf_to_export.keys()
    assert "task_list" in wf_to_export.keys()
    assert len(wf_to_export["task_list"]) == 2


async def test_import_workflow(
    db,
    project_factory,
    MockCurrentUser,
    collect_packages,
    testdata_path,
):

    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user=user)

    with (
        testdata_path
        / "objects_for_db_import_export"
        / "exported_workflow.json"
    ).open("r") as f:
        wf = json.load(f)
    debug(wf)

    # FIXME: by now we go through the pair (source, name), but maybe we should
    # put all together - see issue #293.
    sourcename_to_id = {}
    for wf_task in wf["task_list"]:
        source = wf_task["task"]["source"]
        name = wf_task["task"]["name"]
        if not (source, name) in sourcename_to_id.keys():
            stm = (
                select(Task)
                .where(Task.name == name)
                .where(Task.source == source)
            )
            res = await db.execute(stm)
            tasks = res.scalars().all()
            assert len(tasks) == 1
            sourcename_to_id[(source, name)] = tasks[0].id
    debug(sourcename_to_id)

    # Check that there is no workflow with the same name
    # and same project_id
    stm = (
        select(Workflow)
        .where(Workflow.name == wf["name"])
        .where(Workflow.project_id == prj.id)
    )
    res = await db.execute(stm)
    if res.scalars().all():
        raise RuntimeError

    # Create workflow
    workflow_create = WorkflowCreate(**wf, project_id=prj.id)
    db_workflow = Workflow.from_orm(workflow_create)
    db.add(db_workflow)
    await db.commit()
    await db.refresh(db_workflow)

    # Add WorkflowTask's
    for wf_task in wf["task_list"]:
        source = wf_task["task"]["source"]
        name = wf_task["task"]["name"]
        task_id = sourcename_to_id[(source, name)]
        debug(task_id)
        new_task = WorkflowTaskCreate(
            **wf_task, workflow_id=db_workflow.id, task_id=task_id
        )

        async with db:
            db_workflow_task = await db_workflow.insert_task(
                **new_task.dict(exclude={"workflow_id"}),
                db=db,
                commit=True,
            )
            await db.refresh(db_workflow_task)
            debug(db_workflow_task)

    async with db:
        await db.refresh(db_workflow)
        debug(db_workflow)
