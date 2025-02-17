from devtools import debug

from fractal_server.app.models.v2 import AccountingRecord
from fractal_server.utils import get_timestamp


async def test_accounting(
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser() as user1:
        db.add(
            AccountingRecord(
                user_id=user1.id,
                num_new_images=10,
                num_tasks=20,
            )
        )
        intermediate_time = get_timestamp()
        db.add(
            AccountingRecord(
                user_id=user1.id,
                num_new_images=10,
                num_tasks=20,
            )
        )
        await db.commit()
    async with MockCurrentUser() as user2:
        db.add(
            AccountingRecord(
                user_id=user2.id,
                num_new_images=10,
                num_tasks=20,
            )
        )

    async with MockCurrentUser(user_kwargs=dict(is_superuser=True)):
        res = await client.post(
            "/admin/v2/accounting/",
            json=dict(
                user_id=user1.id,
                timestamp_min=intermediate_time.isoformat(),
            ),
        )
        assert res.status_code == 200
        accounting_list = res.json()
        debug(accounting_list)
        assert res.json()["total_count"] == 1


async def test_accounting_non_superuser(
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser(user_kwargs=dict(is_superuser=False)):
        res = await client.post("/admin/v2/accounting/", json={})
        assert res.status_code == 401
