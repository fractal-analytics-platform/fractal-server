from fractal_server.app.models.v2.workflow_template import WorkflowTemplate


async def test_get_template(db, client, MockCurrentUser):
    async with MockCurrentUser() as user1:
        user1_id = user1.id
        user1_email = user1.email
    async with MockCurrentUser() as user2:
        user2_id = user2.id
        user2_email = user2.email

    fake_workflow_export = dict(name="workflow", description=None, task_list=[])
    db.add_all(
        [
            WorkflowTemplate(
                user_id=user1_id,
                name="template1",
                version=1,
                data=fake_workflow_export,
            ),
            WorkflowTemplate(
                user_id=user1_id,
                name="template1",
                version=2,
                data=fake_workflow_export,
            ),
            WorkflowTemplate(
                user_id=user2_id,
                name="template2",
                version=1,
                data=fake_workflow_export,
            ),
        ]
    )
    await db.commit()

    async with MockCurrentUser():
        res = await client.get("api/v2/workflow_template/")
        assert res.status_code == 200
        assert res.json()["current_page"] == 1
        assert res.json()["page_size"] == 3
        assert res.json()["total_count"] == 3
        items = res.json()["items"]
        assert len(items) == 3
        assert items[0]["user_email"] == user1_email
        assert items[1]["user_email"] == user1_email
        assert items[2]["user_email"] == user2_email
