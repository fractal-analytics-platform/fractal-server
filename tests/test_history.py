import json

from devtools import debug

from fractal_server.app.runner import METADATA_FILENAME


async def test_get_workflowtask_status(
    db,
    MockCurrentUser,
    tmp_path,
    project_factory,
    task_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    client,
):
    """
    FIXME add docstring (with A/B/C)
    """

    RESULTS = dict(done=set(), failed=set(), submitted=set())

    # (A) These statuses will be written in the metadata file, and they will be
    # the final ones - as there existin no corresponding WorkflowTasks
    history_next = []
    for shift, status in enumerate(["done", "failed"]):
        ID = 100 + shift
        history_next.append(dict(workflowtask=dict(id=ID), status=status))
        RESULTS[status].add(ID)

    working_dir = tmp_path / "working_dir"
    working_dir.mkdir()
    with (working_dir / METADATA_FILENAME).open("w") as f:
        tmp_meta = dict(history_next=history_next)
        json.dump(tmp_meta, f)
    debug(working_dir / METADATA_FILENAME)

    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        task = await task_factory(name="task1", source="task1")
        workflow = await workflow_factory(project_id=project.id, name="WF")
        input_dataset = await dataset_factory(project)

        # Prepare output_dataset.meta["history_next"]
        history_next = []

        # (B) The statuses for these IDs will be overwritten by "submitted",
        # because they match with the task_list of the workflow associated to a
        # job associated to output_dataset
        for dummy_status in ["done", "failed", "submitted"]:
            await workflow.insert_task(task_id=task.id, db=db)
            ID = workflow.task_list[-1].id
            history_next.append(
                dict(workflowtask=dict(id=ID), status=dummy_status)
            )
            RESULTS["submitted"].add(ID)
        await db.close()

        # (C) The statuses for these IDs will be the final ones, as there are
        # no corresponding WorkflowTasks
        for shift, status in enumerate(["done", "failed", "submitted"]):
            ID = 200 + shift
            history_next.append(dict(workflowtask=dict(id=ID), status=status))
            RESULTS[status].add(ID)

        # Create output_dataset
        meta = dict(history_next=history_next)
        output_dataset = await dataset_factory(project, meta=meta)

        # Create job in relation with output_dataset and workflow
        job = await job_factory(  # noqa
            project_id=project.id,
            workflow_id=workflow.id,
            input_dataset_id=input_dataset.id,
            output_dataset_id=output_dataset.id,
            working_dir=str(working_dir),
            first_task_index=0,
            last_task_index=3,
        )
        debug(job)

        # Test get_workflowtask_status endpoint
        res = await client.get(
            f"api/v1/project/{project.id}/dataset/{output_dataset.id}/status/"
        )
        debug(res.status_code)
        assert res.status_code == 200
        statuses = res.json()["status"]
        debug(statuses)
        for expected_status, IDs in RESULTS.items():
            for ID in IDs:
                ID_str = str(ID)  # JSON-object keys can only be strings
                assert statuses[ID_str] == expected_status


async def test_export_history_as_workflow_fail(
    db,
    MockCurrentUser,
    tmp_path,
    project_factory,
    task_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    client,
):
    """
    Fail because of existing jobs linked to the dataset
    """
    async with MockCurrentUser(persist=True) as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id, name="WF")
        dataset = await dataset_factory(project)

        # Create job in relation with dataset
        job = await job_factory(  # noqa
            project_id=project.id,
            workflow_id=workflow.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            working_dir=str(tmp_path / "working_dir"),
            first_task_index=0,
            last_task_index=0,
        )

        # Test export_history_as_workflow failure
        res = await client.get(
            f"api/v1/project/{project.id}/"
            f"dataset/{dataset.id}/export_history/"
        )
        assert res.status_code == 422
        debug(res.json()["detail"])
        assert res.json()["detail"].startswith("Cannot export history")

        # Create second job in relation with dataset
        job = await job_factory(  # noqa
            project_id=project.id,
            workflow_id=workflow.id,
            input_dataset_id=dataset.id,
            output_dataset_id=dataset.id,
            working_dir=str(tmp_path / "working_dir"),
            first_task_index=0,
            last_task_index=0,
        )

        # Test export_history_as_workflow failure
        res = await client.get(
            f"api/v1/project/{project.id}/"
            f"dataset/{dataset.id}/export_history/"
        )
        assert res.status_code == 422
        debug(res.json()["detail"])
        assert res.json()["detail"].startswith("Cannot export history")
