import os
from pathlib import Path

import pytest
from fastapi import HTTPException
from pydantic import BaseModel

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

    # Case 1: Undefined logfile
    log = read_log_file(logfile=None, wftask=wftask, dataset_id=1)
    assert "not available" in log

    # Case 2: File does not exist
    logfile = (tmp_path / "logs.txt").as_posix()
    log = read_log_file(logfile=logfile, wftask=wftask, dataset_id=1)
    assert "not available" in log

    # Case 3: File exists and can be read
    with open(logfile, "w") as f:
        f.write("some keyword\n")
    log = read_log_file(logfile=logfile, wftask=wftask, dataset_id=1)
    assert "some keyword" in log

    # Case 4: File exists but cannot be read
    os.chmod(logfile, 0o000)
    log = read_log_file(logfile=logfile, wftask=wftask, dataset_id=1)
    assert "Permission denied" in log
