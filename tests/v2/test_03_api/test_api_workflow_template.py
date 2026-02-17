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
