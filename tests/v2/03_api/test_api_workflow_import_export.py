import json
from typing import Optional

from devtools import debug  # noqa

from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import TaskV2
from fractal_server.app.models import UserGroup
from fractal_server.app.schemas.v2 import TaskImportV2
from fractal_server.app.schemas.v2 import WorkflowImportV2
from fractal_server.app.schemas.v2 import WorkflowReadV2


PREFIX = "api/v2"


async def test_import_export_workflow(
    client,
    MockCurrentUser,
    project_factory_v2,
    task_factory,
    task_factory_v2,
    testdata_path,
):

    # Load workflow to be imported into DB
    with (testdata_path / "import_export/workflow-v2.json").open("r") as f:
        workflow_from_file = json.load(f)

    async with MockCurrentUser() as user:
        # Create project
        prj = await project_factory_v2(user)
        # Add dummy tasks to DB
        await task_factory_v2(
            user_id=user.id, name="task", source="PKG_SOURCE:dummy2"
        )
        await task_factory(name="task", source="PKG_SOURCE:dummy1")

    # Import workflow into project
    payload = WorkflowImportV2(**workflow_from_file).dict(exclude_none=True)

    res = await client.post(
        f"{PREFIX}/project/{prj.id}/workflow/import/", json=payload
    )
    assert res.status_code == 201
    workflow_imported = res.json()

    # Check that output can be cast to WorkflowRead
    WorkflowReadV2(**workflow_imported)

    # Export workflow
    workflow_id = workflow_imported["id"]
    res = await client.get(
        f"{PREFIX}/project/{prj.id}/workflow/{workflow_id}/export/"
    )
    debug(res)
    debug(res.status_code)
    workflow_exported = res.json()
    debug(workflow_exported)

    assert "id" not in workflow_exported
    assert "project_id" not in workflow_exported
    for wftask in workflow_exported["task_list"]:
        assert "id" not in wftask
        assert "task_id" not in wftask
        assert "workflow_id" not in wftask
        assert "id" not in wftask["task"]
    assert res.status_code == 200

    # Check that the exported workflow is an extension of the one in the
    # original JSON file

    # FIXME: we must check that workflow from file is correctly translated in
    # workflow new

    assert len(workflow_from_file["task_list"]) == len(
        workflow_exported["task_list"]
    )

    for task_old, task_new in zip(
        workflow_from_file["task_list"], workflow_exported["task_list"]
    ):
        assert task_old.keys() <= task_new.keys()
        for meta in ["meta_parallel", "meta_non_parallel"]:
            if task_old.get(meta):
                # then 'meta' is also in task_new
                debug(meta)
                assert task_old[meta].items() <= task_new[meta].items()
                task_old.pop(meta)
                task_new.pop(meta)
            elif task_new.get(meta):  # but not in task_old
                task_new.pop(meta)
        debug(task_old, task_new)
        # remove task from task_list item
        # cause task_new.task is a TaskExportV2
        # task_old.task is a TaskExportV2Legacy
        # we remove also import filters because
        # in the wf_old it would be added by the
        # WorkflowExportV2
        task_old.pop("task")
        task_new.pop("task")
        task_new.pop("input_filters")
        assert task_old == task_new


async def test_export_workflow_log(
    client,
    MockCurrentUser,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
):
    """
    WHEN exporting a workflow with custom tasks
    THEN there must be a warning
    """

    # Create project and task
    async with MockCurrentUser() as user:
        TASK_OWNER = "someone"
        task = await task_factory_v2(
            user_id=user.id, owner=TASK_OWNER, source="some-source"
        )
        prj = await project_factory_v2(user)
        wf = await workflow_factory_v2(project_id=prj.id)

    # Insert WorkflowTasks
    res = await client.post(
        (
            f"api/v2/project/{prj.id}/workflow/{wf.id}/wftask/"
            f"?task_id={task.id}"
        ),
        json={},
    )
    assert res.status_code == 201

    # Export workflow
    res = await client.get(
        f"/api/v2/project/{prj.id}/workflow/{wf.id}/export/"
    )
    assert res.status_code == 200


async def test_import_export_workflow_fail(
    client,
    MockCurrentUser,
    project_factory_v2,
    task_factory,
):
    async with MockCurrentUser() as user:
        prj = await project_factory_v2(user)

    await task_factory(name="valid", source="test_source")
    payload = {
        "name": "MyWorkflow",
        "task_list": [{"task": {"source": "xyz"}}],
    }
    res = await client.post(
        f"/api/v2/project/{prj.id}/workflow/import/", json=payload
    )
    assert res.status_code == 422


async def test_new_import_export(
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
        wf = await workflow_factory_v2(project_id=prj.id)

        await task_factory_v2(user_id=user.id, source=wf_file_task_source)

        # Export workflow
        res = await client.get(
            f"/api/v2/project/{prj.id}/workflow/{wf.id}/export/"
        )
        assert res.status_code == 200

        res = await client.post(
            f"{PREFIX}/project/{prj.id}/workflow/import/",
            json=workflow_from_file,
        )
        assert res.status_code == 201

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
            task_group_kwargs=dict(pkg_name="fractal-tasks-core", version="0"),
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


async def test_get_task_by_source():
    from fractal_server.app.routes.api.v2.workflow_import import (
        _get_task_by_source,
    )

    task1 = TaskV2(id=1, name="task1", source="source1")
    task2 = TaskV2(id=2, name="task2", source="source2")
    task_group = [
        TaskGroupV2(
            task_list=[task1, task2],
            user_id=1,
            user_group_id=1,
            pkg_name="pkg1",
            version="1.0",
        )
    ]

    # Test matching source
    task_id = await _get_task_by_source("source1", task_group)
    assert task_id == 1

    # Test non-matching source
    task_id = await _get_task_by_source("nonexistent_source", task_group)
    assert task_id is None


async def test_get_task_by_version():
    from fractal_server.app.routes.api.v2.workflow_import import (
        _get_task_by_version,
    )

    task1 = TaskV2(id=1, name="task1")
    task2 = TaskV2(id=2, name="task2")
    task_group1 = TaskGroupV2(
        task_list=[task1],
        user_id=1,
        user_group_id=1,
        pkg_name="pkg1",
        version="1.0",
    )
    task_group2 = TaskGroupV2(
        task_list=[task2],
        user_id=2,
        user_group_id=2,
        pkg_name="pkg1",
        version="2.0",
    )
    task_groups = [task_group1, task_group2]
    task_import = TaskImportV2(name="task1", pkg_name="pkg1")

    # Test with matching version
    task_id = await _get_task_by_version(task_import, 1, None, task_groups, 1)
    assert task_id == 1

    # Test with non-matching version
    task_import.version = "3.0"
    task_id = await _get_task_by_version(task_import, 1, None, task_groups, 1)
    assert task_id is None


async def test_disambiguate_task_groups(MockCurrentUser, task_factory_v2, db):
    from fractal_server.app.routes.api.v2.workflow_import import (
        _disambiguate_task_groups,
    )

    task1 = TaskV2(id=1, name="task1")
    task2 = TaskV2(id=2, name="task2")
    task_group1 = TaskGroupV2(
        task_list=[task1],
        user_id=1,
        user_group_id=1,
        pkg_name="pkg1",
        version="1.0",
    )
    task_group2 = TaskGroupV2(
        task_list=[task2],
        user_id=2,
        user_group_id=2,
        pkg_name="pkg1",
        version="2.0",
    )

    matching_task_groups = [task_group1, task_group2]
    default_group_id = 1

    async with MockCurrentUser() as user:
        await task_factory_v2(user_id=user.id)

        result = await _disambiguate_task_groups(
            matching_task_groups, user.id, db, default_group_id
        )
        debug(result)
        assert result == task_group1
