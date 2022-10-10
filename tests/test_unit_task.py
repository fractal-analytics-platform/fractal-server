import asyncio

from devtools import debug
from sqlmodel import select

from fractal_server.app.models import Subtask
from fractal_server.app.models import SubtaskRead
from fractal_server.app.models import Task
from fractal_server.app.models import TaskRead
from fractal_server.app.models.project import LinkTaskProject


async def test_add_subtask(db, task_factory):
    parent = await task_factory(name="parent")
    child = await task_factory(name="child")
    otherchild = await task_factory(name="otherchild")

    await parent.add_subtask(db, subtask=child)
    debug(TaskRead.from_orm(parent))

    assert len(parent.subtask_list) == 1
    assert parent.subtask_list[0].subtask == child

    # insert otherchild at the beginning
    await parent.add_subtask(db, subtask=otherchild, order=0)
    assert parent.subtask_list[0].subtask == otherchild
    assert parent.subtask_list[1].subtask == child


async def test_task_relations(db, task_factory):
    """
    GIVEN two tasks
    WHEN a child task is added to a parent task
    THEN the relationship is correctly established in the database
    """
    parent = await task_factory(name="parent")
    child0 = await task_factory(name="child0")
    child1 = await task_factory(name="child1")

    db.add(parent)
    db.add(child0)
    child0_subtask = Subtask(parent=parent, subtask=child0)
    child1_subtask = Subtask(parent=parent, subtask=child1)
    db.add(child0_subtask)
    db.add(child1_subtask)
    await db.commit()
    await db.refresh(parent)
    await db.refresh(child0)
    await db.refresh(child0_subtask)
    await db.refresh(child1_subtask)
    debug(parent)
    assert child0_subtask.order == 0
    assert child1_subtask.order == 1
    assert len(parent.subtask_list) == 2
    assert parent.subtask_list[0].subtask == child0
    assert parent.subtask_list[0].subtask_id == child0.id

    subtask_read = SubtaskRead.from_orm(child0_subtask)
    child0_read = TaskRead.from_orm(child0)
    debug(subtask_read)
    assert subtask_read.subtask == child0_read

    # Swap children and check order
    child1_pop = parent.subtask_list.pop(1)
    parent.subtask_list.insert(0, child1_pop)
    db.add_all([parent, child0, child1])
    await db.commit()
    await asyncio.gather(
        *[db.refresh(item) for item in [parent, child0, child1]]
    )
    await db.commit()
    debug(parent)
    assert child0_subtask.order == 1
    assert child1_subtask.order == 0


async def test_task_parameter_override(db, task_factory):
    """
    GIVEN a parent workflow and a child task
    WHEN the parent workflow defines overrides for the task's parameters
    THEN
        * the information is saved correctly in the database
        * the merged parameters are readily accessible to the executor
    """
    default_args = dict(a=1, b=2)
    override_args = dict(a="overridden")
    t = await task_factory(default_args=default_args)
    parent = await task_factory(
        resource_type="task", name="parent", subtask_list=[t]
    )
    parent.subtask_list[0].args = override_args
    db.add(parent)
    await db.commit()
    await db.refresh(parent)

    debug(parent)
    assert parent.subtask_list[0]._arguments["a"] == override_args["a"]
    assert parent.subtask_list[0]._arguments["b"] == default_args["b"]


async def test_arguments_executor(db, task_factory):
    EXPECTED = "my executor"
    default_args = dict(a=1, executor=EXPECTED)
    t = await task_factory(default_args=default_args)

    assert "executor" not in t._arguments
    assert t.executor == EXPECTED

    pt = t.preprocess()[0]
    debug(pt)

    assert pt.executor == EXPECTED
    assert "executor" not in pt._arguments
    assert "executor" not in pt.args


async def test_arguments_parallelization_level(db, task_factory):
    EXPECTED = "my par level"
    default_args = dict(a=1, parallelization_level=EXPECTED)
    t = await task_factory(default_args=default_args)

    assert "parallelization_level" not in t._arguments
    assert t.parallelization_level == EXPECTED

    pt = t.preprocess()[0]
    debug(pt)

    assert pt.parallelization_level == EXPECTED
    assert "parallelization_level" not in pt._arguments
    assert "parallelization_level" not in pt.args


async def test_unit_task_project_relationship(
    db, task_factory, project_factory, MockCurrentUser
):
    async with MockCurrentUser(persist=True) as user:
        t = await task_factory()
        p = await project_factory(user)

    p.task_list.append(t)
    db.add(p)
    await db.commit()
    await db.refresh(t)

    stm = (
        select(LinkTaskProject)
        .where(LinkTaskProject.project_id == p.id)
        .where(LinkTaskProject.task_id == t.id)
    )
    res = await db.execute(stm)
    obj = res.scalars().all()
    assert len(obj) == 1

    debug(p)
    assert p.task_list == [t]

    debug(t)
    t = await db.get(Task, t.id)

    assert t.project_list == [p]
