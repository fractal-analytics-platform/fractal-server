from fractal_server.app.runner.v2.handle_failed_job import (
    mark_last_wftask_as_failed,
)


async def test_unit_mark_last_wftask_as_failed(
    db,
    dataset_factory_v2,
    project_factory_v2,
    MockCurrentUser,
    caplog,
):
    async with MockCurrentUser() as user:
        project = await project_factory_v2(user=user)
        dataset_no_history = await dataset_factory_v2(
            project_id=project.id,
            name="name",
            history=[],
        )

        caplog.clear()
        mark_last_wftask_as_failed(
            dataset_id=dataset_no_history.id, logger_name="logger"
        )
        print(caplog.text)
        assert "is empty. Likely reason" in caplog.text

        dataset_wrong_history = await dataset_factory_v2(
            name="name",
            history=[
                {
                    "workflowtask": {"id": 123},
                    "status": "done",
                }
            ],
        )

        caplog.clear()
        mark_last_wftask_as_failed(
            dataset_id=dataset_wrong_history.id, logger_name="logger"
        )
        print(caplog.text)
        assert "Unexpected branch: Last history item" in caplog.text
