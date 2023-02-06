from devtools import debug

from fractal_server.app.models import Workflow


async def test_export_workflow(
    db,
    project_factory,
    MockCurrentUser,
    collect_packages,
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
