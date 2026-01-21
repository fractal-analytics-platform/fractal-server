import json

from devtools import debug  # noqa

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models import UserGroup
from fractal_server.app.schemas.v2 import TaskImport

PREFIX = "api/v2"


async def test_import_export(
    client,
    MockCurrentUser,
    task_factory,
    project_factory,
    testdata_path,
    db,
):
    with (testdata_path / "import_export/workflow-v2.json").open("r") as f:
        workflow_from_file = json.load(f)

    # Aux function
    def wf_modify(task_import: dict, new_name: str | None = None):
        wf_from_file = workflow_from_file.copy()
        wf_from_file["name"] = new_name
        wf_from_file["task_list"][0]["task"] = task_import
        return wf_from_file

    task0 = workflow_from_file["task_list"][0]["task"]
    task1 = workflow_from_file["task_list"][1]["task"]

    async with MockCurrentUser() as user:
        prj = await project_factory(user)
        task_0 = await task_factory(
            user_id=user.id,
            name=task0["name"],
            version=task0["version"],
            task_group_kwargs=dict(pkg_name=task0["pkg_name"]),
        )
        task_1 = await task_factory(
            user_id=user.id,
            name=task1["name"],
            version=task1["version"],
            task_group_kwargs=dict(pkg_name=task1["pkg_name"]),
        )

        # Import fail
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json={
                "name": "Name",
                "task_list": [
                    {"task": {"name": "x", "pkg_name": "y", "version": "z"}}
                ],
            },
        )
        assert res.status_code == 422
        assert "Could not find a task matching with" in res.json()["details"]

        # Import workflow
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=workflow_from_file,
        )
        assert res.status_code == 201
        workflow_imported = res.json()
        assert len(workflow_imported["task_list"]) == len(
            workflow_from_file["task_list"]
        )
        workflow_imported_id = workflow_imported["id"]
        assert workflow_imported["task_list"][0]["task"]["id"] == task_0.id
        assert workflow_imported["task_list"][1]["task"]["id"] == task_1.id

        # Export the workflow we just imported
        res = await client.get(
            f"/api/v2/project/{prj.id}/workflow/{workflow_imported_id}/export/"
        )
        workflow_exported = res.json()
        assert len(workflow_exported["task_list"]) == len(
            workflow_from_file["task_list"]
        )

        # Exported workflow has no database IDs
        assert "id" not in workflow_exported
        assert "project_id" not in workflow_exported
        for wftask in workflow_exported["task_list"]:
            assert "id" not in wftask
            assert "task_id" not in wftask
            assert "workflow_id" not in wftask
            assert "id" not in wftask["task"]
            assert "taskgroupv2_id" not in wftask["task"]
        assert res.status_code == 200

        # Invalid request: both source and the other attributes
        invalid_payload = wf_modify(
            {
                "source": "fractal-tasks-core-1.2.3-cellpose",
                "pkg_name": "fractal-tasks-core",
                "version": "1.2.3",
                "name": "cellpose_segmentation",
            }
        )
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/", json=invalid_payload
        )
        assert res.status_code == 422

        # Invalid request: no source, and missing pkg_name
        invalid_payload = wf_modify(
            {
                "pkg_name": "fractal-tasks-core",
                "version": "1.2.3",
            }
        )
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/", json=invalid_payload
        )
        assert res.status_code == 422

        # Invalid request: empty dictionary
        invalid_payload = wf_modify({})
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/", json=invalid_payload
        )
        assert res.status_code == 422

        first_task_no_source = await task_factory(
            user_id=user.id,
            name="cellpose_segmentation",
            task_group_kwargs=dict(
                pkg_name="fractal-tasks-core",
                version="0",
                active=False,
            ),
        )

        # Valid request: task-import based
        valid_payload_full = wf_modify(
            new_name="foo",
            task_import={
                "pkg_name": "fractal-tasks-core",
                "version": "0",
                "name": "cellpose_segmentation",
            },
        )
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=valid_payload_full,
        )
        assert res.status_code == 201
        assert (
            res.json()["task_list"][0]["task"]["taskgroupv2_id"]
            == first_task_no_source.id
        )
        assert res.json()["task_list"][0]["warning"] is not None

        valid_payload_miss_version = wf_modify(
            new_name="foo2",
            task_import={
                "pkg_name": "fractal-tasks-core",
                "name": "cellpose_segmentation",
            },
        )
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=valid_payload_miss_version,
        )
        assert res.status_code == 201

        # Add task no version latest group
        # Test the disambiguation based on the oldest UserGroup
        # add a new group and a new task associated with that user group
        # check that during the import phase will be choosen the oldest
        # not the latest task
        new_group = UserGroup(name="new_group")
        db.add(new_group)
        await db.commit()
        await db.refresh(new_group)
        link = LinkUserGroup(user_id=user.id, group_id=new_group.id)
        db.add(link)
        await db.commit()
        await db.close()

        await task_factory(
            user_id=user.id,
            name="cellpose_segmentation",
            task_group_kwargs=dict(
                user_id=user.id,
                version=None,
                user_group_id=new_group.id,
                pkg_name="fractal-tasks-core",
            ),
        )

        valid_payload_miss_version_latest_group = wf_modify(
            new_name="foo3",
            task_import={
                "pkg_name": "fractal-tasks-core",
                "name": "cellpose_segmentation",
            },
        )
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=valid_payload_miss_version_latest_group,
        )
        assert res.status_code == 201

        assert (
            res.json()["task_list"][0]["task"]["taskgroupv2_id"]
            == first_task_no_source.taskgroupv2_id
        )


async def test_unit_get_task_by_taskimport():
    from fractal_server.app.routes.api.v2.workflow_import import (
        _get_task_by_taskimport,
    )

    task1 = TaskV2(id=1, name="task")
    task2 = TaskV2(id=2, name="task")
    task3 = TaskV2(id=3, name="task")

    task_group1 = TaskGroupV2(
        task_list=[task1],
        user_id=1,
        user_group_id=1,
        pkg_name="pkg",
        version="1.0.0",
    )
    task_group2 = TaskGroupV2(
        task_list=[task2],
        user_id=1,
        user_group_id=2,
        pkg_name="pkg",
        version="2.0.0",
    )
    task_group3 = TaskGroupV2(
        task_list=[task3],
        user_id=1,
        user_group_id=2,
        pkg_name="pkg",
        version=None,
    )
    task_groups = [task_group1, task_group2, task_group3]

    # Test with matching version
    task_id = await _get_task_by_taskimport(
        task_import=TaskImport(name="task", pkg_name="pkg", version="1.0.0"),
        user_id=1,
        task_groups_list=task_groups,
        default_group_id=1,
        db=None,
    )
    assert task_id == task1.id

    # Test with latest version
    task_id = await _get_task_by_taskimport(
        task_import=TaskImport(
            name="task",
            pkg_name="pkg",
        ),
        user_id=1,
        task_groups_list=task_groups,
        default_group_id=1,
        db=None,
    )
    assert task_id == task2.id

    # Test with latest version equal to None
    task_id = await _get_task_by_taskimport(
        task_import=TaskImport(
            name="task",
            pkg_name="pkg",
        ),
        user_id=1,
        task_groups_list=[task_group3],
        default_group_id=1,
        db=None,
    )
    assert task_id == task3.id

    # Test with non-matching version
    task_id = await _get_task_by_taskimport(
        task_import=TaskImport(
            name="task",
            pkg_name="pkg",
            version="invalid",
        ),
        user_id=1,
        task_groups_list=task_groups,
        default_group_id=1,
        db=None,
    )
    assert task_id is None

    # Test with non-matching pkg_name
    task_id = await _get_task_by_taskimport(
        task_import=TaskImport(
            name="task",
            pkg_name="invalid",
        ),
        user_id=1,
        task_groups_list=task_groups,
        default_group_id=1,
        db=None,
    )
    assert task_id is None

    # Test with non-matching name
    task_id = await _get_task_by_taskimport(
        task_import=TaskImport(
            name="invalid",
            pkg_name="pkg",
        ),
        user_id=1,
        task_groups_list=task_groups,
        default_group_id=1,
        db=None,
    )
    assert task_id is None


async def test_unit_disambiguate_task_groups(
    MockCurrentUser,
    task_factory,
    db,
    default_user_group,
):
    import time

    from fractal_server.app.routes.api.v2._aux_task_group_disambiguation import (  # noqa
        _disambiguate_task_groups,
    )

    async with MockCurrentUser() as user1:
        user1_id = user1.id
    async with MockCurrentUser() as user2:
        user2_id = user2.id
    async with MockCurrentUser() as user3:
        user3_id = user3.id

    old_group = UserGroup(name="Old group")
    db.add(old_group)
    await db.commit()
    await db.refresh(old_group)
    time.sleep(0.1)
    new_group = UserGroup(name="New group")
    db.add(new_group)
    await db.commit()
    await db.refresh(new_group)

    db.add(LinkUserGroup(user_id=user1_id, group_id=old_group.id))
    db.add(LinkUserGroup(user_id=user2_id, group_id=old_group.id))
    db.add(LinkUserGroup(user_id=user3_id, group_id=old_group.id))
    db.add(LinkUserGroup(user_id=user1_id, group_id=new_group.id))
    db.add(LinkUserGroup(user_id=user2_id, group_id=new_group.id))
    db.add(LinkUserGroup(user_id=user3_id, group_id=new_group.id))
    await db.commit()

    task_A = await task_factory(
        name="taskA",
        user_id=user1_id,
        task_group_kwargs=dict(
            pkg_name="pkg",
            version="1.0.0",
            user_group_id=default_user_group.id,
        ),
    )

    task_B = await task_factory(
        name="taskB",
        user_id=user2_id,
        task_group_kwargs=dict(
            pkg_name="pkg",
            version="1.0.0",
            user_group_id=old_group.id,
        ),
    )

    task_C = await task_factory(
        name="taskC",
        user_id=user3_id,
        task_group_kwargs=dict(
            pkg_name="pkg",
            version="1.0.0",
            user_group_id=new_group.id,
        ),
    )

    task_group_A = await db.get(TaskGroupV2, task_A.taskgroupv2_id)
    task_group_B = await db.get(TaskGroupV2, task_B.taskgroupv2_id)
    task_group_C = await db.get(TaskGroupV2, task_C.taskgroupv2_id)

    await db.close()

    # Pick task-group owned by user
    task_group = await _disambiguate_task_groups(
        matching_task_groups=[task_group_A, task_group_B],
        user_id=user1_id,
        default_group_id=default_user_group.id,
        db=db,
    )
    debug(task_group)
    assert task_group.id == task_group_A.id

    # Pick task-group related to "All" user group
    task_group = await _disambiguate_task_groups(
        matching_task_groups=[task_group_A, task_group_C],
        user_id=user2_id,
        default_group_id=default_user_group.id,
        db=db,
    )
    debug(task_group)
    assert task_group.id == task_group_A.id
    user_group = await db.get(UserGroup, task_group.user_group_id)
    assert user_group.name == "All"

    # Out of non-"All" user groups, pick task group corresponding to
    # oldest link
    task_group = await _disambiguate_task_groups(
        matching_task_groups=[task_group_B, task_group_C],
        user_id=user1_id,
        default_group_id=default_user_group.id,
        db=db,
    )
    debug(task_group)
    assert task_group.id == task_group_B.id
    user_group = await db.get(UserGroup, task_group.user_group_id)
    assert user_group.name == "Old group"

    # Unreachable edge case with []
    task_group = await _disambiguate_task_groups(
        matching_task_groups=[],
        user_id=user2_id,
        default_group_id=default_user_group.id,
        db=db,
    )
    debug(task_group)
    assert task_group is None


async def test_import_with_legacy_filters(
    client,
    MockCurrentUser,
    task_factory,
    project_factory,
):
    async with MockCurrentUser() as user:
        prj = await project_factory(user)
        ENDPOINT_URL = f"{PREFIX}/project/{prj.id}/workflow/import/"
        task = await task_factory(
            name="mytask",
            version="myversion",
            user_id=user.id,
        )
        TYPE_FILTERS = dict(key1=True, key2=False)
        payload = {
            "name": "myworkflow0",
            "task_list": [
                {
                    # Task with new type filters
                    "task": {
                        "name": task.name,
                        "pkg_name": task.name,
                        "version": task.version,
                    },
                    "type_filters": TYPE_FILTERS,
                },
                # Task with new type filters and filters=None
                {
                    "task": {
                        "name": task.name,
                        "pkg_name": task.name,
                        "version": task.version,
                    },
                    "type_filters": TYPE_FILTERS,
                    "input_filters": None,
                },
                {
                    # Task with legacy filters
                    "task": {
                        "name": task.name,
                        "pkg_name": task.name,
                        "version": task.version,
                    },
                    "input_filters": {"types": TYPE_FILTERS, "attributes": {}},
                },
            ],
        }
        res = await client.post(ENDPOINT_URL, json=payload)
        assert res.status_code == 201
        for wft in res.json()["task_list"]:
            assert "input_filters" not in wft.keys()
            assert wft["type_filters"] == TYPE_FILTERS

        # FAILURE: legacy and new filters cannot coexist
        payload = {
            "name": "myworkflow1",
            "task_list": [
                {
                    "task": {
                        "name": task.name,
                        "pkg_name": task.name,
                        "version": task.version,
                    },
                    "type_filters": TYPE_FILTERS,
                    "input_filters": {
                        "types": TYPE_FILTERS,
                        "attributes": {},
                    },
                }
            ],
        }
        res = await client.post(ENDPOINT_URL, json=payload)
        debug(res.json())
        assert res.status_code == 422
        assert "Cannot set filters both through the legacy" in str(res.json())

        # FAILURE: invalid type filter
        payload = {
            "name": "myworkflow2",
            "task_list": [
                {
                    "task": {
                        "name": task.name,
                        "pkg_name": task.name,
                        "version": task.version,
                    },
                    "input_filters": {
                        "types": {"key1": "not-a-boolean"},
                        "attributes": {},
                    },
                }
            ],
        }
        res = await client.post(ENDPOINT_URL, json=payload)
        debug(res.json())
        assert res.status_code == 422
        assert "should be a valid boolean" in str(res.json())

        # FAILURE: Attribute filters are now deprecated
        payload = {
            "name": "myworkflow3",
            "task_list": [
                {
                    "task": {
                        "name": task.name,
                        "pkg_name": task.name,
                        "version": task.version,
                    },
                    "input_filters": {
                        "types": {},
                        "attributes": {"key1": "value1"},
                    },
                }
            ],
        }
        res = await client.post(ENDPOINT_URL, json=payload)
        debug(res.json())
        assert res.status_code == 422
        assert "Cannot set attribute filters for WorkflowTasks." in str(
            res.json()
        )


async def test_import_filters_compatibility(
    MockCurrentUser,
    project_factory,
    task_factory,
    client,
):
    async with MockCurrentUser() as user:
        prj = await project_factory(user)
        await task_factory(
            user_id=user.id,
            input_types={"a": True, "b": False},
            name="foo",
            version="0.0.1",
        )

        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=dict(
                name="Workflow Ok",
                task_list=[
                    {
                        "task": {
                            "name": "foo",
                            "pkg_name": "foo",
                            "version": "0.0.1",
                        },
                        "type_filters": {"a": True},
                    }
                ],
            ),
        )
        assert res.status_code == 201

        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=dict(
                name="Workflow Fail",
                task_list=[
                    {
                        "task": {
                            "name": "foo",
                            "pkg_name": "foo",
                            "version": "0.0.1",
                        },
                        "type_filters": {"b": True},
                    }
                ],
            ),
        )
        assert res.status_code == 422
        assert "filters" in res.json()["detail"]


async def test_import_multiple_task_groups_same_version(
    client,
    MockCurrentUser,
    task_factory,
    project_factory,
    db,
):
    """
    Represent regression described in
    https://github.com/fractal-analytics-platform/fractal-server/issues/2852
    """

    TASK_NAME = "my-task"
    PKG_NAME = "tasks"
    V1 = "1.0.0"
    V2 = "2.0.0"

    async with MockCurrentUser() as user2:
        user2_id = user2.id

    some_usergroup = UserGroup(name="Some group of users")
    db.add(some_usergroup)
    await db.commit()
    await db.refresh(some_usergroup)

    await task_factory(
        user_id=user2.id,
        name=TASK_NAME,
        task_group_kwargs=dict(
            pkg_name=PKG_NAME,
            version=V2,
        ),
        version=V2,
    )

    async with MockCurrentUser() as user1:
        proj = await project_factory(user1)

        db.add(LinkUserGroup(user_id=user1.id, group_id=some_usergroup.id))
        db.add(LinkUserGroup(user_id=user2_id, group_id=some_usergroup.id))
        await db.commit()

        await task_factory(
            user_id=user1.id,
            name=TASK_NAME,
            task_group_kwargs=dict(
                pkg_name=PKG_NAME,
                version=V1,
            ),
            version=V1,
        )
        await task_factory(
            user_id=user1.id,
            name=TASK_NAME,
            task_group_kwargs=dict(
                pkg_name=PKG_NAME,
                version=V2,
                user_group_id=some_usergroup.id,
            ),
            version=V2,
        )
        res = await client.post(
            f"{PREFIX}/project/{proj.id}/workflow/import/",
            json=dict(
                name="name",
                task_list=[
                    dict(
                        task=dict(
                            pkg_name=PKG_NAME,
                            version=V2,
                            name=TASK_NAME,
                        )
                    )
                ],
            ),
        )
        assert res.json()["task_list"][0]["task"]["version"] == V2
