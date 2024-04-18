import json

import pytest
from devtools import debug

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)
from fractal_server.app.runner.filenames import HISTORY_FILENAME


async def test_get_workflowtask_status(
    db,
    MockCurrentUser,
    tmp_path,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    client,
):
    """
    This test reflects the behavior of
    `/project/{project_id}/dataset/{dataset_id}/status/` which gives different
    priority to different sources. From lowest to highest priority:

    * Statuses already present in `dataset.history`, in the
        database;
    * "submitted" status for all task in the current job;
    * Temporary-file contents.
    """

    RESULTS = dict(done=set(), failed=set(), submitted=set())

    # (A) These statuses will be written in the history file, and they will be
    # the final ones - as there exist no corresponding WorkflowTasks
    history = []
    for shift, status in enumerate(["done", "failed"]):
        ID = 100 + shift
        history.append(dict(workflowtask=dict(id=ID), status=status))
        RESULTS[status].add(ID)

    working_dir = tmp_path / "working_dir"
    working_dir.mkdir()
    with (working_dir / HISTORY_FILENAME).open("w") as f:
        json.dump(history, f)
    debug(working_dir / HISTORY_FILENAME)

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        task = await task_factory_v2(name="task1", source="task1")
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")

        # Prepare dataset.history
        history = []

        # (B) The statuses for these IDs will be overwritten by "submitted",
        # because they match with the task_list of the workflow associated to a
        # job associated to dataset
        for dummy_status in ["done", "failed", "submitted"]:
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task.id, db=db
            )
            ID = workflow.task_list[-1].id
            history.append(dict(workflowtask=dict(id=ID), status=dummy_status))
            RESULTS["submitted"].add(ID)
        await db.close()

        # (C) The statuses for these IDs will be the final ones, as there are
        # no corresponding WorkflowTasks
        for shift, status in enumerate(["done", "failed", "submitted"]):
            ID = 200 + shift
            history.append(dict(workflowtask=dict(id=ID), status=status))
            RESULTS[status].add(ID)

        # Create dataset and job
        dataset = await dataset_factory_v2(
            project_id=project.id, history=history
        )
        await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=str(working_dir),
        )

        # Test get_workflowtask_status endpoint
        res = await client.get(
            f"api/v2/project/{project.id}/dataset/{dataset.id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        debug(RESULTS)
        for expected_status, IDs in RESULTS.items():
            for ID in IDs:
                ID_str = str(ID)  # JSON-object keys can only be strings
                if ID_str in ["100", "101", "202"]:
                    # because we remove from the respose body all the tasks
                    # after the last failed.
                    with pytest.raises(KeyError):
                        statuses[ID_str] == expected_status
                else:
                    assert statuses[ID_str] == expected_status


async def test_get_workflowtask_status_simple(
    db,
    MockCurrentUser,
    tmp_path,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    client,
):
    """
    Same as test_get_workflowtask_status, but without any temporary history
    file in `working_dir`.
    """

    RESULTS = dict(done=set(), failed=set(), submitted=set())
    working_dir = tmp_path / "working_dir"

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        task = await task_factory_v2(name="task1", source="task1")
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")

        # (B) The statuses for these IDs will be overwritten by "submitted",
        # because they match with the task_list of the workflow associated to a
        # job associated to dataset
        history = []
        for dummy_status in ["done", "failed", "submitted"]:
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task.id, db=db
            )
            ID = workflow.task_list[-1].id
            history.append(dict(workflowtask=dict(id=ID), status=dummy_status))
            RESULTS["submitted"].add(ID)
        await db.close()

        # (C) The statuses for these IDs will be the final ones, as there are
        # no corresponding WorkflowTasks
        for shift, status in enumerate(["done", "failed", "submitted"]):
            ID = 200 + shift
            history.append(dict(workflowtask=dict(id=ID), status=status))
            RESULTS[status].add(ID)

        # Create output_dataset and job
        meta = dict()
        history = history
        dataset = await dataset_factory_v2(
            project_id=project.id, meta=meta, history=history
        )
        await job_factory_v2(
            project_id=project.id,
            workflow_id=workflow.id,
            dataset_id=dataset.id,
            working_dir=str(working_dir),
        )

        # Test get_workflowtask_status endpoint
        res = await client.get(
            f"api/v2/project/{project.id}/dataset/{dataset.id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        for expected_status, IDs in RESULTS.items():

            for ID in IDs:
                ID_str = str(ID)  # JSON-object keys can only be strings
                if ID_str == "202":
                    # because we remove from the respose body all the tasks
                    # after the last failed.
                    with pytest.raises(KeyError):
                        statuses[ID_str] == expected_status
                else:
                    assert statuses[ID_str] == expected_status


async def test_get_workflowtask_status_fail(
    db,
    MockCurrentUser,
    tmp_path,
    project_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflow_factory_v2,
    job_factory_v2,
    client,
):
    """
    Fail due to multiple ongoing jobs being associated with a given dataset
    """

    async with MockCurrentUser() as user:
        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id, name="WF")
        task = await task_factory_v2()
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        dataset = await dataset_factory_v2(project_id=project.id)

        # Create *two* jobs in relation with dataset
        for _ in range(2):
            await job_factory_v2(  # noqa
                project_id=project.id,
                workflow_id=workflow.id,
                dataset_id=dataset.id,
                working_dir=str(tmp_path / "working_dir"),
            )

        # Test export_history_as_workflow failure
        res = await client.get(
            f"api/v2/project/{project.id}/dataset/{dataset.id}/status/"
        )
        assert res.status_code == 422
        debug(res.json()["detail"])
