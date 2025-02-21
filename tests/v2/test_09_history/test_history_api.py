from fractal_server.app.models.v2 import HistoryItemV2


async def test_delete_workflow_associated_to_history(
    project_factory_v2,
    workflow_factory_v2,
    task_factory_v2,
    dataset_factory_v2,
    workflowtask_factory_v2,
    db,
    client,
    MockCurrentUser,
    tmp_path,
):
    async with MockCurrentUser() as user:

        # Create project
        project = await project_factory_v2(user)
        dataset = await dataset_factory_v2(project_id=project.id)
        workflow = await workflow_factory_v2(project_id=project.id)
        task = await task_factory_v2(user_id=user.id)
        wftask = await workflowtask_factory_v2(
            workflow_id=workflow.id, task_id=task.id
        )

        history = HistoryItemV2(
            dataset_id=dataset.id,
            workflowtask_id=wftask.id,
            worfklowtask_dump={},
            task_group_dump={},
            parameters_hash="abc",
            num_available_images=0,
            num_current_images=0,
            images={},
        )
        db.add(history)
        await db.commit()

        await db.refresh(history)
        assert history.workflowtask_id == wftask.id

        await client.delete(
            f"/api/v2/project/{project.id}/workflow/{workflow.id}/"
        )

        await db.refresh(history)
        assert history.workflowtask_id is None
