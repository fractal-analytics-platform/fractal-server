from datetime import datetime
from datetime import timezone
from urllib.parse import quote

from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)


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
):
    async with MockCurrentUser(
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
                workflow_id=wf.id,
                dataset_id=ds.id,
                status="failed",
                working_dir="/fake",
                last_task_index=0,
                start_timestamp=datetime(2000, 1, 1, tzinfo=timezone.utc),
            )

    async with MockCurrentUser(
        profile_id=slurm_ssh_resource_profile_fake_db[1].id
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
            workflow_id=wf.id,
            dataset_id=ds.id,
            status="failaaed",
            working_dir="/fake",
            last_task_index=0,
        )

    await user_group_factory("groupA", user1_id)
    await user_group_factory("groupB", user1_id, user2_id)

    async with MockCurrentUser(
        is_superuser=True,
        profile_id=local_resource_profile_db[1].id,
    ):
        res = await client.get("/admin/v2/users-csv/")
        assert res.status_code == 200

        start_timestamp_min = quote("3000-01-01T00:00:01+00:00")
        res = await client.get(
            f"/admin/v2/users-csv/?start_timestamp_min={start_timestamp_min}"
        )
        assert res.status_code == 200

        res = await client.get(
            f"/admin/v2/users-csv/?exclude_zero_jobs=true&start_timestamp_min={start_timestamp_min}"
        )
        assert res.status_code == 200

        res = await client.get("/admin/v2/users-csv/?exclude_zero_jobs=true")
        assert res.status_code == 200
