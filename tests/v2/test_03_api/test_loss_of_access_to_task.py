from fractal_server.app.models import LinkUserGroup
from fractal_server.app.models import UserGroup
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.api.v2._aux_functions import (
    _workflow_insert_task,
)


async def test_loss_of_access_to_task(
    MockCurrentUser,
    task_factory_v2,
    project_factory_v2,
    workflow_factory_v2,
    dataset_factory_v2,
    db,
    client,
    local_resource_profile_db,
):
    """
    Test the loss-of-access-to-task scenario described in
    https://github.com/fractal-analytics-platform/fractal-server/issues/1840
    """
    resource, profile = local_resource_profile_db
    for i, remove in enumerate(["user_from_group", "group"]):
        # Create two users and a group
        attrs = dict(
            hashed_password="x",
            is_active=True,
            is_superuser=False,
            is_verified=True,
            profile_id=profile.id,
        )
        user_A = UserOAuth(email=f"a{i}@a.a", **attrs)
        user_B = UserOAuth(email=f"b{i}@b.b", **attrs)
        team_group = UserGroup(name=f"team{i}")
        db.add(user_A)
        db.add(user_B)
        db.add(team_group)
        await db.commit()
        await db.refresh(user_A)
        await db.refresh(user_B)
        await db.refresh(team_group)

        # Add users to group
        db.add(LinkUserGroup(user_id=user_A.id, group_id=team_group.id))
        db.add(LinkUserGroup(user_id=user_B.id, group_id=team_group.id))
        await db.commit()

        # Create tasks with different ownership info
        task_A = await task_factory_v2(
            command_non_parallel="echo",
            user_id=user_A.id,
            name=f"iteration-{i}-A",
        )
        task_B = await task_factory_v2(
            command_non_parallel="echo",
            user_id=user_B.id,
            task_group_kwargs=dict(user_group_id=team_group.id),
            name=f"iteration-{i}-B",
        )
        async with MockCurrentUser(user_kwargs=dict(id=user_A.id)) as user:
            # Prepare all objects
            project = await project_factory_v2(user)
            dataset = await dataset_factory_v2(
                project_id=project.id,
                zarr_dir="/fake/",
                images=[dict(zarr_url="/fake/1")],
            )
            workflow = await workflow_factory_v2(project_id=project.id)
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task_A.id, db=db
            )
            await _workflow_insert_task(
                workflow_id=workflow.id, task_id=task_B.id, db=db
            )

            # User A can get task_B any more
            res = await client.get(f"/api/v2/task/{task_B.id}/")
            assert res.status_code == 200

            # Successfully run a job
            res = await client.post(
                f"/api/v2/project/{project.id}/job/submit/"
                f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
                json={},
            )
            assert res.status_code == 202
            job_id = res.json()["id"]
            res = await client.get(
                f"/api/v2/project/{project.id}/job/{job_id}/"
            )
            assert res.status_code == 200
            assert res.json()["status"] == "done"

        if remove == "user_from_group":
            # Remove user_A from team group
            link = await db.get(LinkUserGroup, (team_group.id, user_A.id))
            assert link is not None
            await db.delete(link)
            await db.commit()
            link = await db.get(LinkUserGroup, (team_group.id, user_A.id))
            assert link is None
        elif remove == "group":
            # Remove the team group
            async with MockCurrentUser(user_kwargs=dict(is_superuser=True)):
                res = await client.delete(f"/auth/group/{team_group.id}/")
                assert res.status_code == 204
        else:
            raise RuntimeError("Wrong branch.")

        async with MockCurrentUser(user_kwargs=dict(id=user_A.id)) as user:
            # User A cannot get task_B any more
            res = await client.get(f"/api/v2/task/{task_B.id}/")
            assert res.status_code == 403

            # Job submission fails
            res = await client.post(
                f"/api/v2/project/{project.id}/job/submit/"
                f"?workflow_id={workflow.id}&dataset_id={dataset.id}",
                json={},
            )
            assert res.status_code == 403
            assert "Current user has no read access" in str(
                res.json()["detail"]
            )

            # Workflow is still visible (with warnings)
            res = await client.get(
                f"/api/v2/project/{project.id}/workflow/{workflow.id}/"
            )
            assert res.status_code == 200
            wftasks = res.json()["task_list"]
            assert wftasks[0]["warning"] is None
            assert (
                wftasks[1]["warning"]
                == "Current user has no access to this task."
            )
