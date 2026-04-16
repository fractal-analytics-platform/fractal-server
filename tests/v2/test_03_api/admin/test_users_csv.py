import io
from datetime import datetime
from datetime import timezone
from urllib.parse import quote

from fractal_server.app.routes.admin.v2.users_csv import _COLUMN_NAMES
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)

COLUMNS_HEADER = ",".join(_COLUMN_NAMES)


async def test_users_csv(
    db,
    client,
    MockCurrentUser,
    project_factory,
    workflow_factory,
    dataset_factory,
    task_factory,
    job_factory,
    user_group_factory,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_db,
    tmp_path,
):
    async with MockCurrentUser(
        user_email="user1@example.org",
        project_dirs=["/tmp1", "/tmp2", "/tmp3"],
        slurm_accounts=["account1", "account2"],
        profile_id=local_resource_profile_db[1].id,
    ) as user1:
        user1_id = user1.id
        t = await task_factory(user_id=user1.id, name="1")
        p = await project_factory(user1)
        wf = await workflow_factory(project_id=p.id)
        await _workflow_insert_task(
            workflow_id=wf.id, task_id=t.id, db=db, order=0
        )
        ds = await dataset_factory(project_id=p.id)
        for ind in range(4):
            await job_factory(
                project_id=p.id,
                user_email=user1.email,
                workflow_id=wf.id,
                dataset_id=ds.id,
                status="failed",
                working_dir="/fake",
                last_task_index=0,
                start_timestamp=datetime(2000, 1, 1, tzinfo=timezone.utc),
            )

    async with MockCurrentUser(
        user_email="user2@example.org",
        profile_id=slurm_ssh_resource_profile_fake_db[1].id,
    ) as user2:
        user2_id = user2.id
        t = await task_factory(user_id=user2.id, name="2")
        p = await project_factory(user2)
        wf = await workflow_factory(project_id=p.id)
        await _workflow_insert_task(
            workflow_id=wf.id, task_id=t.id, db=db, order=0
        )
        ds = await dataset_factory(project_id=p.id)
        await job_factory(
            project_id=p.id,
            user_email=user2.email,
            workflow_id=wf.id,
            dataset_id=ds.id,
            status="failaaed",
            working_dir="/fake",
            last_task_index=0,
        )

    await user_group_factory("groupA", user1_id)
    await user_group_factory("groupB", user1_id, user2_id)

    async def _get_csv_response(url: str) -> str:
        # Auxiliary function
        res = await client.get(url)
        assert res.status_code == 200
        assert "text/csv" in res.headers.get("content-type")
        with io.BytesIO() as csv_data:
            csv_data.write(res.content)
            return csv_data.getvalue().decode()

    async with MockCurrentUser(
        is_superuser=True,
        user_email="admin@example.org",
    ):
        # Case 1: All users
        url = "/admin/v2/users-csv/"
        data = await _get_csv_response(url)
        assert data == (
            f"{COLUMNS_HEADER}\r\n"
            "3,admin@example.org,,,/fake/placeholder,All,0\r\n"
            "1,user1@example.org,,account1|account2,/tmp1|/tmp2|/tmp3,All|groupA|groupB,4\r\n"
            "2,user2@example.org,test01,,/fake/placeholder,All|groupB,1\r\n"
        )

        # Case 2: All users, but only counting jobs after some future time
        future_timestamp = quote("3000-01-01T00:00:01+00:00")
        url = f"/admin/v2/users-csv/?start_timestamp_min={future_timestamp}"
        data = await _get_csv_response(url)
        assert data == (
            f"{COLUMNS_HEADER}\r\n"
            "3,admin@example.org,,,/fake/placeholder,All,0\r\n"
            "1,user1@example.org,,account1|account2,/tmp1|/tmp2|/tmp3,All|groupA|groupB,0\r\n"
            "2,user2@example.org,test01,,/fake/placeholder,All|groupB,0\r\n"
        )

        # Case 3: All users, but only counting jobs before some past time
        past_timestamp = quote("1000-01-01T00:00:01+00:00")
        url = f"/admin/v2/users-csv/?start_timestamp_max={past_timestamp}"
        data = await _get_csv_response(url)
        assert data == (
            f"{COLUMNS_HEADER}\r\n"
            "3,admin@example.org,,,/fake/placeholder,All,0\r\n"
            "1,user1@example.org,,account1|account2,/tmp1|/tmp2|/tmp3,All|groupA|groupB,0\r\n"
            "2,user2@example.org,test01,,/fake/placeholder,All|groupB,0\r\n"
        )

        # Case 4: Only users with jobs (at any time)
        url = "/admin/v2/users-csv/?exclude_zero_jobs=true"
        data = await _get_csv_response(url)
        assert data == (
            f"{COLUMNS_HEADER}\r\n"
            "1,user1@example.org,,account1|account2,/tmp1|/tmp2|/tmp3,All|groupA|groupB,4\r\n"
            "2,user2@example.org,test01,,/fake/placeholder,All|groupB,1\r\n"
        )
