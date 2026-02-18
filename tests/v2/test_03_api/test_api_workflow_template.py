from fractal_server.app.models.v2.workflow_template import WorkflowTemplate
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)

WORKFLOW_EXPORT_MOCK = dict(name="workflow", description=None, task_list=[])


async def test_get_template(db, client, MockCurrentUser, user_group_factory):
    async with MockCurrentUser() as user1:
        user1_id = user1.id
        user1_email = user1.email
    async with MockCurrentUser() as user2:
        user2_id = user2.id
        user2_email = user2.email

    group = await user_group_factory("group", user1.id, user2.id, db=db)

    template1 = WorkflowTemplate(
        user_id=user1_id,
        name="template",
        version=1,
        data=WORKFLOW_EXPORT_MOCK,
    )
    template2 = WorkflowTemplate(
        user_id=user1_id,
        name="other",
        version=2,
        data=WORKFLOW_EXPORT_MOCK,
    )
    template3 = WorkflowTemplate(
        user_id=user2_id,
        user_group_id=group.id,
        name="template2",
        version=1,
        data=WORKFLOW_EXPORT_MOCK,
    )
    template4 = WorkflowTemplate(
        user_id=user2_id,
        name="template2",
        version=2,
        data=WORKFLOW_EXPORT_MOCK,
    )
    db.add_all([template1, template2, template3, template4])
    await db.commit()
    await db.refresh(template1)
    await db.refresh(template2)
    await db.refresh(template3)
    await db.refresh(template4)

    async with MockCurrentUser(user_id=user1_id):
        res = await client.get("api/v2/workflow_template/")
        assert res.status_code == 200
        assert res.json()["current_page"] == 1
        assert res.json()["page_size"] == 3
        assert res.json()["total_count"] == 3
        items = res.json()["items"]
        assert len(items) == 3
        assert items[0]["id"] == template1.id
        assert items[0]["user_email"] == user1_email
        assert items[1]["id"] == template2.id
        assert items[1]["user_email"] == user1_email
        assert items[2]["id"] == template3.id
        assert items[2]["user_email"] == user2_email
        # TODO: test sorting
        # Test pagination
        res = await client.get("api/v2/workflow_template/?page_size=2&page=2")
        assert res.status_code == 200
        assert res.json()["current_page"] == 2
        assert res.json()["page_size"] == 2
        assert res.json()["total_count"] == 3
        items = res.json()["items"]
        assert len(items) == 1
        assert items[0]["id"] == template3.id
        # Filter by `is_owner`
        res = await client.get("api/v2/workflow_template/?is_owner=true")
        assert res.status_code == 200
        items = res.json()["items"]
        assert len(items) == 2
        assert items[0]["id"] == template1.id
        assert items[1]["id"] == template2.id
        # Filter by `user_email`
        res = await client.get(
            f"api/v2/workflow_template/?user_email={user2_email}"
        )
        assert res.status_code == 200
        items = res.json()["items"]
        assert len(items) == 1
        assert items[0]["id"] == template3.id
        # Filter by `name`
        res = await client.get("api/v2/workflow_template/?name=template")
        assert res.status_code == 200
        items = res.json()["items"]
        assert len(items) == 2
        assert items[0]["id"] == template1.id
        assert items[1]["id"] == template3.id
        # Filter by `version`
        res = await client.get("api/v2/workflow_template/?version=2")
        assert res.status_code == 200
        items = res.json()["items"]
        assert len(items) == 1
        assert items[0]["id"] == template2.id
        # Test GET single template
        res = await client.get(f"api/v2/workflow_template/{template2.id}/")
        assert res.status_code == 200
        assert res.json()["user_email"] == user1_email
        res = await client.get(f"api/v2/workflow_template/{template3.id}/")
        assert res.status_code == 200
        res = await client.get(f"api/v2/workflow_template/{template4.id}/")
        assert res.status_code == 403
        res = await client.get("api/v2/workflow_template/9999/")
        assert res.status_code == 404


async def test_post_patch_delete_template(
    project_factory,
    workflow_factory,
    task_factory,
    user_group_factory,
    MockCurrentUser,
    client,
    db,
):
    async with MockCurrentUser() as user0:
        group0 = await user_group_factory("group0", user0.id, db=db)
        group0_id = group0.id
        template0 = WorkflowTemplate(
            user_id=user0.id,
            name="template0",
            version=1,
            data=WORKFLOW_EXPORT_MOCK,
        )
        db.add(template0)
        await db.commit()
        await db.refresh(template0)
        template0_id = template0.id

    async with MockCurrentUser() as user1:
        group1 = await user_group_factory("group1", user1.id, db=db)
        group2 = await user_group_factory("group2", user1.id, db=db)
        project = await project_factory(user1)
        workflow = await workflow_factory(project_id=project.id, name="foo")
        task = await task_factory(user_id=user1.id, name="my_task")
        await _workflow_insert_task(
            workflow_id=workflow.id, task_id=task.id, db=db
        )
        # Test POST
        res = await client.post(
            f"api/v2/workflow_template/?workflow_id={workflow.id}",
            json=dict(name="template", version=1),
        )
        assert res.status_code == 201
        assert res.json()["user_email"] == user1.email
        assert res.json()["name"] == "template"
        assert res.json()["version"] == 1
        assert res.json()["user_group_id"] is None
        assert res.json()["description"] is None
        assert res.json()["data"]["name"] == "foo"
        assert res.json()["data"]["task_list"][0]["task"]["name"] == "my_task"
        # Test POST duplicate
        res = await client.post(
            f"api/v2/workflow_template/?workflow_id={workflow.id}",
            json=dict(name="template", version=1),
        )
        assert res.status_code == 422
        assert "There is already a WorkflowTemplate" in res.json()["detail"]
        # Test POST with `user_group_id`
        res = await client.post(
            "api/v2/workflow_template/"
            f"?workflow_id={workflow.id}&user_group_id=9999",
            json=dict(name="template", version=2),
        )
        assert res.status_code == 404
        res = await client.post(
            "api/v2/workflow_template/"
            f"?workflow_id={workflow.id}&user_group_id={group0_id}",
            json=dict(name="template", version=2),
        )
        assert res.status_code == 403
        res = await client.post(
            "api/v2/workflow_template/"
            f"?workflow_id={workflow.id}&user_group_id={group1.id}",
            json=dict(name="template", version=2),
        )
        assert res.status_code == 201
        assert res.json()["user_group_id"] == group1.id
        assert res.json()["description"] is None
        template1_id = res.json()["id"]
        # Test PATCH
        res = await client.patch("api/v2/workflow_template/9999/", json=dict())
        assert res.status_code == 404
        assert "not found" in res.json()["detail"]
        res = await client.patch(
            f"api/v2/workflow_template/{template0_id}/",
            json=dict(),
        )
        assert res.status_code == 403
        assert "not the owner" in res.json()["detail"]
        res = await client.patch(
            f"api/v2/workflow_template/{template1_id}/",
            json=dict(user_group_id=group0_id),
        )
        assert res.status_code == 403
        assert "not belong to UserGroup" in res.json()["detail"]
        res = await client.patch(
            f"api/v2/workflow_template/{template1_id}/",
            json=dict(user_group_id=group2.id, description="description"),
        )
        assert res.status_code == 200
        assert res.json()["user_group_id"] == group2.id
        assert res.json()["description"] == "description"
        # Test DELETE
        res = await client.delete("api/v2/workflow_template/9999/")
        assert res.status_code == 404
        assert "not found" in res.json()["detail"]
        res = await client.delete(f"api/v2/workflow_template/{template0_id}/")
        assert res.status_code == 403
        assert "not the owner" in res.json()["detail"]
        res = await client.delete(f"api/v2/workflow_template/{template1_id}/")
        assert res.status_code == 204
        template1 = await db.get(WorkflowTemplate, template1_id)
        assert template1 is None


async def test_export_import_template(
    project_factory,
    workflow_factory,
    user_group_factory,
    MockCurrentUser,
    client,
    db,
):
    async with MockCurrentUser() as user0:
        user0_id = user0.id

    async with MockCurrentUser() as user:
        group = await user_group_factory("group1", user.id, user0_id, db=db)
        project = await project_factory(user)
        workflow = await workflow_factory(project_id=project.id, name="foo")
        res = await client.post(
            "api/v2/workflow_template/"
            f"?workflow_id={workflow.id}&user_group_id={group.id}",
            json=dict(name="template", version=1),
        )
        template_id = res.json()["id"]

        # Export
        res = await client.get(
            f"api/v2/workflow_template/{template_id}/export/"
        )
        assert res.status_code == 200
        template_file = res.json()
        assert template_file == {
            "name": "template",
            "version": 1,
            "description": None,
            "data": {
                "name": "foo",
                "description": None,
                "task_list": [],
            },
        }

        # Import
        res = await client.post(
            f"api/v2/workflow_template/import/?user_group_id={group.id}",
            json=template_file,
        )
        assert res.status_code == 422
        assert "There is already a WorkflowTemplate" in res.json()["detail"]
        template_file["version"] = 2
        res = await client.post(
            f"api/v2/workflow_template/import/?user_group_id={group.id}",
            json=template_file,
        )
        assert res.status_code == 201

    async with MockCurrentUser(user_id=user0_id) as user0:
        project2 = await project_factory(user0)
        res = await client.post(
            f"api/v2/project/{project2.id}/workflow/import-from-template/"
            f"?template_id={template_id}"
        )
        assert res.status_code == 201
        assert res.json()["name"] == "foo"
