from fractal_server.app.models.v2.workflow_template import WorkflowTemplate


async def test_get_template(db, client, MockCurrentUser):
    async with MockCurrentUser() as user1:
        user1_id = user1.id
        user1_email = user1.email
    async with MockCurrentUser() as user2:
        user2_id = user2.id
        user2_email = user2.email

    fake_workflow_export = dict(name="workflow", description=None, task_list=[])
    template1 = WorkflowTemplate(
        user_id=user1_id,
        name="template",
        version=1,
        data=fake_workflow_export,
    )
    template2 = WorkflowTemplate(
        user_id=user1_id,
        name="other",
        version=2,
        data=fake_workflow_export,
    )
    template3 = WorkflowTemplate(
        user_id=user2_id,
        name="template2",
        version=1,
        data=fake_workflow_export,
    )
    db.add_all([template1, template2, template3])
    await db.commit()
    await db.refresh(template1)
    await db.refresh(template2)
    await db.refresh(template3)

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
        res = await client.get(f"api/v2/workflow_template/{template3.id}/")
        assert res.status_code == 200
        assert res.json()["user_email"] == user2_email
        res = await client.get("api/v2/workflow_template/9999/")
        assert res.status_code == 404


async def test_post_template(
    project_factory,
    workflow_factory,
    user_group_factory,
    MockCurrentUser,
    client,
    db,
):
    async with MockCurrentUser() as user0:
        group0 = await user_group_factory("group0", user0.id, db=db)
        group0_id = group0.id

    async with MockCurrentUser() as user1:
        group1 = await user_group_factory("group1", user1.id, db=db)
        project = await project_factory(user1)
        workflow = await workflow_factory(project_id=project.id, name="foo")
        #
        res = await client.post(
            f"api/v2/workflow_template/?workflow_id={workflow.id}",
            json=dict(name="template", version=1),
        )
        assert res.status_code == 201
        assert res.json()["user_email"] == user1.email
        assert res.json()["name"] == "template"
        assert res.json()["version"] == 1
        assert res.json()["user_group_id"] is None
        assert res.json()["data"]["name"] == "foo"
        # Test duplicate
        res = await client.post(
            f"api/v2/workflow_template/?workflow_id={workflow.id}",
            json=dict(name="template", version=1),
        )
        assert res.status_code == 422
        assert "There is already a WorkflowTemplate" in res.json()["detail"]
        # Test `user_group_id`
        res = await client.post(
            f"api/v2/workflow_template/?workflow_id={workflow.id}",
            json=dict(name="template", version=2, user_group_id=9999),
        )
        assert res.status_code == 404
        res = await client.post(
            f"api/v2/workflow_template/?workflow_id={workflow.id}",
            json=dict(name="template", version=2, user_group_id=group0_id),
        )
        assert res.status_code == 403
        res = await client.post(
            f"api/v2/workflow_template/?workflow_id={workflow.id}",
            json=dict(name="template", version=2, user_group_id=group1.id),
        )
        assert res.status_code == 201
        assert res.json()["user_group_id"] == group1.id
