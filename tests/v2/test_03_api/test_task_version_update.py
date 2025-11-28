from packaging.version import parse


async def test_get_workflow_version_update_candidates(
    MockCurrentUser,
    project_factory,
    workflow_factory,
    workflowtask_factory,
    task_factory,
    client,
    db,
):
    async with MockCurrentUser() as user:
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id)

        # Matching tasks (0, 1, 2)
        task0 = await task_factory(
            user_id=user.id,
            version="1",
            task_group_kwargs={"pkg_name": "my_pkg", "version": "1"},
            name="my_task",
            args_schema_parallel={"foo": "bar"},
        )
        task1 = await task_factory(
            user_id=user.id,
            version="2",
            task_group_kwargs={"pkg_name": "my_pkg", "version": "2"},
            name="my_task",
            args_schema_parallel={"foo": "bar"},
        )
        task2 = await task_factory(
            user_id=user.id,
            version="3",
            task_group_kwargs={"pkg_name": "my_pkg", "version": "3"},
            name="my_task",
            args_schema_parallel={"foo": "bar"},
        )
        task3 = await task_factory(
            user_id=user.id,
            version="4",
            task_group_kwargs={"pkg_name": "my_pkg", "version": "4"},
            name="my_task",
            args_schema_parallel={"foo": "bar"},
        )
        # Task with no args schemas
        task4 = await task_factory(
            user_id=user.id,
            version="5",
            task_group_kwargs={"pkg_name": "my_pkg", "version": "5"},
            name="my_task",
        )
        # Task with non-parsable version
        task5 = await task_factory(
            user_id=user.id,
            name="my_task",
            args_schema_parallel={"foo": "bar"},
            version="non-parsable-version",
            task_group_kwargs={
                "pkg_name": "my_pkg",
                "version": "non-parsable-version",
            },
        )
        # Task with non-matching pkg_name
        task6 = await task_factory(
            user_id=user.id,
            version="6",
            task_group_kwargs={"pkg_name": "another-one", "version": "6"},
            name="my_task",
            type="converter_compound",
            args_schema_parallel={"foo": "bar"},
        )
        # Task with non-compatible type
        task7 = await task_factory(
            user_id=user.id,
            version="7",
            task_group_kwargs={"pkg_name": "my_pkg", "version": "6"},
            type="parallel",
            name="my_task",
            args_schema_parallel={"foo": "bar"},
        )
        assert task0.type != task7.type
        # Non-active task
        task6 = await task_factory(
            user_id=user.id,
            version="8",
            task_group_kwargs={
                "pkg_name": "my_pkg",
                "version": "8",
                "active": False,
            },
            name="my_task",
            args_schema_parallel={"foo": "bar"},
        )

        for task in [
            task0,
            task1,
            task2,
            task3,
            task4,
            task5,
            task6,
            task7,
        ]:
            await workflowtask_factory(workflow_id=workflow.id, task_id=task.id)

        await db.refresh(workflow)

        res = await client.get(
            f"api/v2/project/{project.id}/workflow/{workflow.id}/"
            "version-update-candidates/",
        )

        assert res.status_code == 200
        assert len(res.json()) == len(workflow.task_list)
        assert res.json() == [
            [
                {"task_id": task1.id, "version": task1.version},
                {"task_id": task2.id, "version": task2.version},
                {"task_id": task3.id, "version": task3.version},
            ],
            [
                {"task_id": task2.id, "version": task2.version},
                {"task_id": task3.id, "version": task3.version},
            ],
            [
                {"task_id": task3.id, "version": task3.version},
            ],
            [],
            [],
            [],
            [],
            [],
        ]


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
        "0.10.0alpha",
        "0.1.2",
        "0.1.dev27+g1458b59",
        "0.2.0a0",
        "0.10.0a",
    ]
    ordered_versions = sorted(
        versions,
        key=lambda _version: parse(_version),
    )

    # NOTE: several versions are considered identical, e.g.
    # "0.10.0a0", "0.10.0alpha0", "0.10.0alpha", "0.10.0a"
    expected_sorted_versions = [
        "0.1.dev27+g1458b59",
        "0.1.2",
        "0.2.0a0",
        "0.10.0a0",
        "0.10.0alpha0",
        "0.10.0alpha",
        "0.10.0a",
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

    assert ordered_versions == expected_sorted_versions
