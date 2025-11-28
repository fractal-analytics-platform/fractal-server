import os
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

from fractal_server.app.routes.api.v2._aux_functions_history import (
    _verify_workflow_and_dataset_access,
)
from fractal_server.app.routes.api.v2._aux_functions_history import (
    get_history_unit_or_404,
)
from fractal_server.app.routes.api.v2._aux_functions_history import (
    read_log_file,
)
from fractal_server.app.schemas.v2.sharing import ProjectPermissions
from fractal_server.zip_tools import _create_zip


async def test_get_history_unit_or_404(db):
    with pytest.raises(
        HTTPException,
        match="999 not found",
    ):
        await get_history_unit_or_404(
            history_unit_id=999,
            db=db,
        )


def test_read_log_file(tmp_path: Path):
    class MockTask(BaseModel):
        name: str = "task-name"

    class MockWorkflowTask(BaseModel):
        task: MockTask

    wftask = MockWorkflowTask(task=MockTask())

    logfile = (tmp_path / "logs.txt").as_posix()

    # Case 1: files do not exist
    log = read_log_file(
        logfile=logfile,
        task_name=wftask.task.name,
        dataset_id=1,
        job_working_dir="/foo",
    )
    assert "not available" in log

    LOG = "some keyword\n"
    with open(logfile, "w") as f:
        f.write(LOG)
    # Case 2: logfile exists and can be read
    log = read_log_file(
        logfile=logfile,
        task_name=wftask.task.name,
        dataset_id=1,
        job_working_dir="/foo.zip",
    )
    assert log == LOG

    # Case 3: File exists but cannot be read
    os.chmod(logfile, 0o000)
    log = read_log_file(
        logfile=logfile,
        task_name=wftask.task.name,
        dataset_id=1,
        job_working_dir="/foo.zip",
    )
    assert "Error while retrieving logs for task" in log

    # Case 4: File exists inside an archive
    os.chmod(logfile, 0o777)
    _create_zip(tmp_path.as_posix(), f"{tmp_path}.zip")
    os.unlink(logfile)
    log = read_log_file(
        logfile=logfile,
        task_name=wftask.task.name,
        dataset_id=1,
        job_working_dir=tmp_path.as_posix(),
    )
    assert log == LOG

    # Case 5: File doesn't exist even inside the archive
    log = read_log_file(
        logfile=logfile + "xxx",
        task_name=wftask.task.name,
        dataset_id=1,
        job_working_dir=tmp_path.as_posix(),
    )
    assert "Error while retrieving logs for task" in log


async def test_verify_workflow_and_dataset_access(
    db,
    project_factory,
    workflow_factory,
    dataset_factory,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        project1 = await project_factory(user=user)
        wf1 = await workflow_factory(project_id=project1.id)
        ds1 = await dataset_factory(project_id=project1.id)

        res = await _verify_workflow_and_dataset_access(
            project_id=project1.id,
            workflow_id=wf1.id,
            dataset_id=ds1.id,
            user_id=user.id,
            required_permissions=ProjectPermissions.EXECUTE,
            db=db,
        )
        assert res["dataset"].id == ds1.id
        assert res["workflow"].id == wf1.id

        project2 = await project_factory(user=user)
        wf2 = await workflow_factory(project_id=project2.id)
        ds2 = await dataset_factory(project_id=project2.id)

        with pytest.raises(HTTPException, match="Workflow does not belong"):
            await _verify_workflow_and_dataset_access(
                project_id=project1.id,
                workflow_id=wf2.id,
                dataset_id=ds1.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )

        with pytest.raises(HTTPException, match="Dataset does not belong"):
            await _verify_workflow_and_dataset_access(
                project_id=project1.id,
                workflow_id=wf1.id,
                dataset_id=ds2.id,
                user_id=user.id,
                required_permissions=ProjectPermissions.EXECUTE,
                db=db,
            )
