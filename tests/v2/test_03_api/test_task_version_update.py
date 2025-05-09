from fractal_server.app.routes.api.v2.task_version_update import TaskVersion


async def test_get_workflow_version_update_candidates(
    MockCurrentUser,
    project_factory_v2,
    workflow_factory_v2,
    workflowtask_factory_v2,
    task_factory_v2,
    client,
    db,
):
    async with MockCurrentUser() as user:

        project = await project_factory_v2(user)
        workflow = await workflow_factory_v2(project_id=project.id)

        task0 = await task_factory_v2(
            user_id=user.id,
            name="my_task",
            args_schema_parallel={"foo": "bar"},
            task_group_kwargs={"pkg_name": "my_pkg", "version": "1.0.0a0"},
        )
        task1 = await task_factory_v2(
            user_id=user.id,
            name="my_task",
            args_schema_parallel={"foo": "bar"},
            task_group_kwargs={"pkg_name": "my_pkg", "version": "1.0.0a1"},
        )
        task2 = await task_factory_v2(
            user_id=user.id,
            name="my_task",
            args_schema_parallel={"foo": "bar"},
            task_group_kwargs={"pkg_name": "my_pkg", "version": "1.0.0"},
        )
        task3 = await task_factory_v2(
            user_id=user.id,
            name="my_task",
            # no 'args_schema_parallel'
            task_group_kwargs={"pkg_name": "my_pkg", "version": "1.0.1"},
        )
        task4 = await task_factory_v2(
            user_id=user.id,
            name="my_task",
            args_schema_parallel={"foo": "bar"},
            task_group_kwargs={"pkg_name": "my_pkg", "version": "2.0.0rc4"},
        )

        for task_id in [task0.id, task1.id, task2.id, task3.id, task4.id]:
            await workflowtask_factory_v2(
                workflow_id=workflow.id, task_id=task_id
            )

        await db.refresh(workflow)

        res = await client.get(
            f"api/v2/project/{project.id}/workflow/{workflow.id}/"
            "version-update-candidates/",
        )

        assert res.status_code == 200
        assert len(res.json()) == len(workflow.task_list)
        assert res.json()[0] == [
            {"task_id": task1.id, "version": "1.0.0a1"},
            {"task_id": task2.id, "version": "1.0.0"},
            {"task_id": task4.id, "version": "2.0.0rc4"},
        ]
        assert res.json()[1] == [
            {"task_id": task2.id, "version": "1.0.0"},
            {"task_id": task4.id, "version": "2.0.0rc4"},
        ]
        assert res.json()[2] == [
            {"task_id": task4.id, "version": "2.0.0rc4"},
        ]
        assert res.json()[3] == []
        assert res.json()[4] == []


async def test_get_workflow_version_update_candidates_ordering():
    versions = [
        "2",
        "0.10.0c0",
        "0.10.0b4",
        "0.10.0",
        "0.10.0alpha3",
        "0.10.0a2",
        "1.0.0",
        "0.10.0a0",
        "1.0.0rc4.dev7",
        "0.10.0beta5",
        "0.10.0alpha0",
        "0.1.2",
        "0.1.dev27+g1458b59",
        "0.2.0a0",
    ]
    unordered = [
        TaskVersion(task_id=0, version=version) for version in versions
    ]
    ordered = sorted(unordered)

    # FIXME this order differs from
    # https://github.com/fractal-analytics-platform/fractal-web/blob/edfd62660ac20220b14d516eb3f67d826d217b45/components/__tests__/version.test.js
    expected_sorted_versions = [
        "0.1.dev27+g1458b59",
        "0.1.2",
        "0.2.0a0",
        "0.10.0a0",
        "0.10.0alpha0",
        "0.10.0a2",
        "0.10.0alpha3",
        "0.10.0b4",
        "0.10.0beta5",
        "0.10.0c0",
        "0.10.0",
        "1.0.0rc4.dev7",
        "1.0.0",
        "2",
    ]

    assert [task.version for task in ordered] == expected_sorted_versions
