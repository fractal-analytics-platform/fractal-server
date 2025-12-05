from datetime import datetime

from devtools import debug

from fractal_server.app.models.v2 import AccountingRecord
from fractal_server.app.models.v2 import AccountingRecordSlurm
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
                num_new_images=1,
                num_tasks=1,
            )
        )
        intermediate_time = get_timestamp()
        db.add(
            AccountingRecord(
                user_id=user1.id,
                num_new_images=2,
                num_tasks=2,
            )
        )
        await db.commit()
    async with MockCurrentUser() as user2:
        db.add(
            AccountingRecord(
                user_id=user2.id,
                num_new_images=3,
                num_tasks=3,
            )
        )

    async with MockCurrentUser(is_superuser=True):
        # Test timestamp/user filters
        res = await client.post(
            "/admin/v2/accounting/",
            json=dict(
                user_id=user1.id,
                timestamp_min=intermediate_time.isoformat(),
                timestamp_max=get_timestamp().isoformat(),
            ),
        )
        assert res.status_code == 200
        accounting_list = res.json()
        debug(accounting_list)
        assert res.json()["total_count"] == 1

        # Test pagination
        res = await client.post("/admin/v2/accounting/?page_size=1", json={})
        assert res.status_code == 200
        assert res.json()["total_count"] == 3
        assert res.json()["page_size"] == 1
        assert res.json()["current_page"] == 1
        assert res.json()["items"][0]["num_tasks"] == 1
        res = await client.post(
            "/admin/v2/accounting/?page_size=1&page=2", json={}
        )
        assert res.status_code == 200
        assert res.json()["total_count"] == 3
        assert res.json()["page_size"] == 1
        assert res.json()["current_page"] == 2
        assert res.json()["items"][0]["num_tasks"] == 2
        res = await client.post(
            "/admin/v2/accounting/?page_size=1&page=3", json={}
        )
        assert res.status_code == 200
        assert res.json()["total_count"] == 3
        assert res.json()["page_size"] == 1
        assert res.json()["current_page"] == 3
        assert res.json()["items"][0]["num_tasks"] == 3


async def test_accounting_api_failure(
    db,
    client,
    MockCurrentUser,
):
    # Non admin
    async with MockCurrentUser(is_superuser=False):
        res = await client.post("/admin/v2/accounting/", json={})
        assert res.status_code == 401

    # Naive datetime
    naive_datetime = datetime.now().isoformat()
    async with MockCurrentUser(is_superuser=True):
        res = await client.post(
            "/admin/v2/accounting/",
            json=dict(
                timestamp_min=naive_datetime,
            ),
        )
        assert res.status_code == 422
        assert "Input should have timezone info" in str(res.json())

    # Pagination error
    async with MockCurrentUser(is_superuser=True):
        res = await client.post(
            "/admin/v2/accounting/?page=2",
            json={},
        )
        assert res.status_code == 422
        assert "Invalid pagination parameters" in str(res.json())


async def test_accounting_slurm(
    db,
    client,
    MockCurrentUser,
):
    async with MockCurrentUser(is_superuser=True) as user:
        timestamp_min = get_timestamp().isoformat()
        db.add(AccountingRecordSlurm(user_id=user.id, slurm_job_ids=[1, 4]))
        db.add(AccountingRecordSlurm(user_id=user.id, slurm_job_ids=[2, 3]))
        timestamp_max = get_timestamp().isoformat()
        await db.commit()
        res = await client.post(
            "/admin/v2/accounting/slurm/",
            json=dict(
                timestamp_max=timestamp_max,
                timestamp_min=timestamp_min,
                user_id=user.id,
            ),
        )
        assert res.status_code == 200
        debug(res.json())
        assert set(res.json()) == {1, 2, 3, 4}
