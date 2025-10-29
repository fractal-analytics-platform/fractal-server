import pytest
from sqlalchemy.exc import IntegrityError

from fractal_server.app.models.v2 import Resource
from fractal_server.app.schemas.v2 import ResourceType


async def test_resource_constraints(db):
    r1 = Resource(
        type=ResourceType.LOCAL,
        name="foo",
        jobs_local_dir="/jobs_local_dir",
        tasks_local_dir="/tasks_local_dir",
        jobs_poll_interval=1,
    )
    db.add(r1)
    await db.commit()

    # test name unique
    r2 = Resource(
        type=ResourceType.LOCAL,
        name="foo",
        jobs_local_dir="/jobs_local_dir2",
        tasks_local_dir="/tasks_local_dir2",
        jobs_poll_interval=1,
    )
    db.add(r2)
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    await db.rollback()
    assert "UniqueViolation" in e.value.args[0]
    r2 = Resource(
        type=ResourceType.LOCAL,
        name="foo2",
        jobs_local_dir="/jobs_local_dir2",
        tasks_local_dir="/tasks_local_dir2",
        jobs_poll_interval=1,
    )
    db.add(r2)
    await db.commit()

    # test `type` in ResourceType
    r3 = Resource(
        type="xxx",
        name="foo3",
        jobs_local_dir="/jobs_local_dir3",
        tasks_local_dir="/tasks_local_dir3",
        jobs_poll_interval=1,
    )
    db.add(r3)
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    await db.rollback()
    assert "correct_type" in e.value.args[0]

    # test `jobs_slurm_python_worker` is set
    r4 = Resource(
        type=ResourceType.SLURM_SSH,
        name="foo4",
        jobs_local_dir="/jobs_local_dir4",
        tasks_local_dir="/tasks_local_dir4",
        jobs_poll_interval=1,
    )
    db.add(r4)
    with pytest.raises(IntegrityError) as e:
        await db.commit()
    await db.rollback()
    assert "jobs_slurm_python_worker_set" in e.value.args[0]
    r4 = Resource(
        type=ResourceType.SLURM_SSH,
        jobs_slurm_python_worker="/jobs_slurm_python_worker",
        name="foo4",
        jobs_local_dir="/jobs_local_dir4",
        tasks_local_dir="/tasks_local_dir4",
        jobs_poll_interval=1,
    )
    db.add(r4)
    await db.commit()
