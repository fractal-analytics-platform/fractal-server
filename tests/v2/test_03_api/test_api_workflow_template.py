from devtools import debug

from fractal_server.app.models.v2.workflow_template import WorkflowTemplate


async def test_get_template(db, client, MockCurrentUser):
    async with MockCurrentUser() as user1:
        user1_id = user1.id
    async with MockCurrentUser() as user2:
        user2_id = user2.id

    db.add_all(
        [
            WorkflowTemplate(
                user_id=user1_id,
                name="template1",
                version=1,
                data={},
            ),
            WorkflowTemplate(
                user_id=user1_id,
                name="template1",
                version=2,
                data={},
            ),
            WorkflowTemplate(
                user_id=user2_id,
                name="template2",
                version=1,
                data={},
            ),
        ]
    )
    await db.commit()

    async with MockCurrentUser():
        res = await client.get("api/v2/workflow_template/")
        assert res.status_code == 200
        debug(res.json())
