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
    class MockTaskV2(BaseModel):
        name: str = "task-name"

    class MockWorkflowTaskV2(BaseModel):
        task: MockTaskV2

    wftask = MockWorkflowTaskV2(task=MockTaskV2())

    logfile = (tmp_path / "logs.txt").as_posix()

    # Case 1: files do not exist
    log = read_log_file(
        logfile=logfile, wftask=wftask, dataset_id=1, archive_path="/foo.zip"
    )
    assert "not available" in log

    with open(logfile, "w") as f:
        f.write("some keyword\n")
    # Case 2: logfile exists and can be read
    log = read_log_file(
        logfile=logfile, wftask=wftask, dataset_id=1, archive_path="/foo.zip"
    )
    assert "some keyword" in log

    # Case 3: File exists but cannot be read
    os.chmod(logfile, 0o000)
    log = read_log_file(
        logfile=logfile, wftask=wftask, dataset_id=1, archive_path="/foo.zip"
    )
    assert "Permission denied" in log


async def test_verify_workflow_and_dataset_access(
    db,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    MockCurrentUser,
):
    async with MockCurrentUser() as user:
        project1 = await project_factory_v2(user=user)
        wf1 = await workflow_factory_v2(project_id=project1.id)
        ds1 = await dataset_factory_v2(project_id=project1.id)

        res = await _verify_workflow_and_dataset_access(
            project_id=project1.id,
            workflow_id=wf1.id,
            dataset_id=ds1.id,
            user_id=user.id,
            db=db,
        )
        assert res["dataset"].id == ds1.id
        assert res["workflow"].id == wf1.id

        project2 = await project_factory_v2(user=user)
        wf2 = await workflow_factory_v2(project_id=project2.id)
        ds2 = await dataset_factory_v2(project_id=project2.id)

        with pytest.raises(HTTPException, match="Workflow does not belong"):
            await _verify_workflow_and_dataset_access(
                project_id=project1.id,
                workflow_id=wf2.id,
                dataset_id=ds1.id,
                user_id=user.id,
                db=db,
            )

        with pytest.raises(HTTPException, match="Dataset does not belong"):
            await _verify_workflow_and_dataset_access(
                project_id=project1.id,
                workflow_id=wf1.id,
                dataset_id=ds2.id,
                user_id=user.id,
                db=db,
            )
