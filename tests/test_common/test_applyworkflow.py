from datetime import datetime

import pytest
from devtools import debug

from fractal_server.common.schemas import ApplyWorkflowCreate
from fractal_server.common.schemas import ApplyWorkflowRead


def test_apply_workflow_create():
    # Valid ApplyWorkflowCreate instance
    valid_args = dict(worker_init="WORKER INIT")
    job = ApplyWorkflowCreate(**valid_args)
    debug(job)

    with pytest.raises(ValueError) as e:
        job = ApplyWorkflowCreate(first_task_index=-1)
    debug(e)

    with pytest.raises(ValueError) as e:
        job = ApplyWorkflowCreate(last_task_index=-1)
    debug(e)

    with pytest.raises(ValueError) as e:
        job = ApplyWorkflowCreate(first_task_index=2, last_task_index=0)
    debug(e)


def test_apply_workflow_read():
    x = ApplyWorkflowRead(
        id=1,
        project_id=1,
        workflow_id=1,
        input_dataset_id=1,
        output_dataset_id=1,
        start_timestamp="2019-12-23T23:10:11.115310Z",
        status="good",
        workflow_dump=dict(task_list=[]),
    )

    assert isinstance(x.start_timestamp, datetime)
    y = x.sanitised_dict()
    assert isinstance(y["start_timestamp"], str)
