import json
from typing import Optional

from devtools import debug  # noqa

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models import UserGroup
from fractal_server.app.schemas.v2 import TaskImportV2


PREFIX = "api/v2"


async def test_import_export(
    client,
    MockCurrentUser,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    testdata_path,
    db,
):

    with (testdata_path / "import_export/workflow-v2.json").open("r") as f:
        workflow_from_file = json.load(f)

    # Aux function
    def wf_modify(task_import: dict, new_name: Optional[str] = None):
        wf_from_file = workflow_from_file.copy()
        wf_from_file["name"] = new_name
        wf_from_file["task_list"][0]["task"] = task_import
        return wf_from_file

    wf_file_task_source = workflow_from_file["task_list"][0]["task"]["source"]

    async with MockCurrentUser() as user:
        prj = await project_factory_v2(user)
        await task_factory_v2(user_id=user.id, source=wf_file_task_source)

        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=workflow_from_file,
        )
        assert res.status_code == 201
        workflow_imported = res.json()
        workflow_imported_id = workflow_imported["id"]
        assert len(workflow_imported["task_list"]) == len(
            workflow_from_file["task_list"]
        )

        # Export workflow
        res = await client.get(
            f"/api/v2/project/{prj.id}/workflow/"
            f"{workflow_imported_id}/export/"
        )
        workflow_exported = res.json()
        assert len(workflow_exported["task_list"]) == len(
            workflow_from_file["task_list"]
        )

        assert "id" not in workflow_exported
        assert "project_id" not in workflow_exported
        for wftask in workflow_exported["task_list"]:
            assert "id" not in wftask
            assert "task_id" not in wftask
            assert "workflow_id" not in wftask
            assert "id" not in wftask["task"]
        assert res.status_code == 200

        # Check that output can be cast to WorkflowRead
        # WorkflowReadV2(**workflow_imported)

        # Case source and the others
        invalid_payload = wf_modify(
            {
                "source": "fractal-tasks-core-1.2.3-cellpose",
                "pkg_name": "fractal-tasks-core",
                "version": "1.2.3",
                "name": "cellpose_segmentation",
            }
        )

        # No casting to WorkflowImportV2
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/", json=invalid_payload
        )
        assert res.status_code == 422

        # Case no source and missing one of the others
        invalid_payload = wf_modify(
            {
                "pkg_name": "fractal-tasks-core",
                "version": "1.2.3",
            }
        )

        # No casting to WorkflowImportV2
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/", json=invalid_payload
        )
        assert res.status_code == 422

        # Case empty dict
        invalid_payload = wf_modify({})

        # No casting to WorkflowImportV2
        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/", json=invalid_payload
        )
        assert res.status_code == 422

        first_task_no_source = await task_factory_v2(
            user_id=user.id,
            name="cellpose_segmentation",
            task_group_kwargs=dict(
                pkg_name="fractal-tasks-core",
                version="0",
                active=False,
            ),
        )

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

        await task_factory_v2(
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


async def test_unit_get_task_by_source():
    from fractal_server.app.routes.api.v2.workflow_import import (
        _get_task_by_source,
    )

    task1 = TaskV2(id=1, name="task1", source="source1")
    task2 = TaskV2(id=2, name="task2", source="source2")
    task3 = TaskV2(id=3, name="task3", source="source3")
    task_group = [
        TaskGroupV2(
            task_list=[task1, task2],
            user_id=1,
            pkg_name="pkgA",
        ),
        TaskGroupV2(
            task_list=[task3],
            user_id=1,
            pkg_name="pkgB",
        ),
    ]

    # Test matching source
    task_id = await _get_task_by_source("source1", task_group)
    assert task_id == 1

    # Test non-matching source
    task_id = await _get_task_by_source("nonexistent_source", task_group)
    assert task_id is None


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
        task_import=TaskImportV2(name="task", pkg_name="pkg", version="1.0.0"),
        user_id=1,
        task_groups_list=task_groups,
        default_group_id=1,
        db=None,
    )
    assert task_id == task1.id

    # Test with latest version
    task_id = await _get_task_by_taskimport(
        task_import=TaskImportV2(
            name="task",
            pkg_name="pkg",
        ),
        user_id=1,
        task_groups_list=task_groups,
        default_group_id=1,
        db=None,
    )
    assert task_id == task2.id

    # Test with non-matching version
    task_id = await _get_task_by_taskimport(
        task_import=TaskImportV2(
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
        task_import=TaskImportV2(
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
        task_import=TaskImportV2(
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
    task_factory_v2,
    db,
    default_user_group,
):
    import time
    from fractal_server.app.routes.api.v2.workflow_import import (
        _disambiguate_task_groups,
    )

    async with MockCurrentUser() as user1:
        user1_id = user1.id
        task_A = await task_factory_v2(
            name="task",
            user_id=user1_id,
            task_group_kwargs=dict(
                pkg_name="pkg",
                version="1.0.0",
                user_group_id=default_user_group.id,
            ),
        )

    async with MockCurrentUser() as user2:
        user2_id = user2.id

        old_group = UserGroup(name="Old group")
        recent_group = UserGroup(name="Recent group")
        db.add(old_group)
        db.add(recent_group)
        await db.commit()
        await db.refresh(old_group)
        await db.refresh(recent_group)

        db.add(LinkUserGroup(user_id=user2.id, group_id=old_group.id))
        db.add(LinkUserGroup(user_id=user1.id, group_id=old_group.id))
        await db.commit()
        time.sleep(0.1)
        db.add(LinkUserGroup(user_id=user1.id, group_id=recent_group.id))
        db.add(LinkUserGroup(user_id=user2.id, group_id=recent_group.id))
        await db.commit()
        await db.close()

        task_B = await task_factory_v2(
            name="task",
            user_id=user1_id,
            task_group_kwargs=dict(
                pkg_name="pkg",
                version="1.0.0",
                user_group_id=old_group.id,
            ),
        )
        task_C = await task_factory_v2(
            name="task",
            user_id=user1_id,
            task_group_kwargs=dict(
                pkg_name="pkg",
                version="1.0.0",
                user_group_id=recent_group.id,
            ),
        )

    task_group_A = await db.get(TaskGroupV2, task_A.taskgroupv2_id)
    task_group_B = await db.get(TaskGroupV2, task_B.taskgroupv2_id)
    task_group_C = await db.get(TaskGroupV2, task_C.taskgroupv2_id)

    await db.close()

    # Pick task-group owned by user
    task_group = await _disambiguate_task_groups(
        matching_task_groups=[task_group_A, task_group_B, task_group_C],
        user_id=user1_id,
        default_group_id=default_user_group.id,
        db=db,
    )
    debug(task_group)
    assert task_group.id == task_group_A.id

    # Pick task-group related to "All" user group
    task_group = await _disambiguate_task_groups(
        matching_task_groups=[task_group_A, task_group_B, task_group_C],
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
        user_id=user2_id,
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
