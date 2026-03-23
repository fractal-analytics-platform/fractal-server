from devtools import debug

from fractal_server.app.models import LinkUserProjectV2


async def test_admin_patch_project(
    db,
    client,
    MockCurrentUser,
    project_factory,
    workflow_factory,
    dataset_factory,
    workflowtask_factory,
    task_factory,
    default_user_group,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_db,
):
    _, profile = local_resource_profile_db
    _, profile_slurm_ssh = slurm_ssh_resource_profile_fake_db

    async with MockCurrentUser(
        profile_id=profile.id,
        project_dirs=["/private-new", "/shared"],
    ) as new_user:
        new_user_id = new_user.id

    async with MockCurrentUser(
        profile_id=profile.id,
        project_dirs=[
            "/private-old",
            "/shared",
        ],
    ) as user_old:
        user_old_id = user_old.id
        task_private = await task_factory(
            user_id=user_old_id,
            user_group_id=None,
            name="private-2",
            command_non_parallel="echo",
        )
        task_shared = await task_factory(
            user_id=user_old_id,
            user_group_id=default_user_group.id,
            name="shared-2",
            command_non_parallel="echo",
        )

        proj1_wrong_zarr_dir = await project_factory(user=user_old)
        proj2_no_task_access = await project_factory(user=user_old)
        proj3_already_shared = await project_factory(user=user_old)
        proj1_id = proj1_wrong_zarr_dir.id
        proj2_id = proj2_no_task_access.id
        proj3_id = proj3_already_shared.id

        wf1 = await workflow_factory(project_id=proj1_id)
        wf2 = await workflow_factory(project_id=proj2_id)
        wf3 = await workflow_factory(project_id=proj3_id)

        await workflowtask_factory(workflow_id=wf1.id, task_id=task_shared.id)
        await workflowtask_factory(workflow_id=wf2.id, task_id=task_private.id)
        await workflowtask_factory(workflow_id=wf3.id, task_id=task_shared.id)

        ds1 = await dataset_factory(
            project_id=proj1_id, zarr_dir="/private-old/zarr1"
        )
        ds2 = await dataset_factory(
            project_id=proj2_id, zarr_dir="/shared/zarr2"
        )
        ds3 = await dataset_factory(
            project_id=proj3_id, zarr_dir="/shared/zarr3"
        )

        res = await client.post(
            f"/api/v2/project/{proj1_id}/job/submit/?dataset_id={ds1.id}&workflow_id={wf1.id}",
            json={},
        )
        assert res.status_code == 202
        res = await client.post(
            f"/api/v2/project/{proj2_id}/job/submit/?dataset_id={ds2.id}&workflow_id={wf2.id}",
            json={},
        )
        assert res.status_code == 202
        res = await client.post(
            f"/api/v2/project/{proj3_id}/job/submit/?dataset_id={ds3.id}&workflow_id={wf3.id}",
            json={},
        )
        assert res.status_code == 202

    async with MockCurrentUser(
        profile_id=profile_slurm_ssh.id,
        project_dirs=["/shared"],
    ) as user_old_different_resource:
        proj4_wrong_profile = await project_factory(
            user=user_old_different_resource
        )
        proj4_id = proj4_wrong_profile.id
        await workflow_factory(project_id=proj4_id)
        await dataset_factory(project_id=proj4_id, zarr_dir="/shared/zarr4")

    # Fail due to new user's `project_dirs`
    res = await client.PATCH(
        f"/admin/v2/project/{proj1_id}/?user_id={new_user_id}"
    )
    assert res.status_code == 422
    debug(res.json())
    await client.patch(
        f"/auth/users/{new_user_id}/",
        json=dict(
            project_dirs=[
                "/private-new",
                "/shared",
                "/private-old",
            ]
        ),
    )
    res = await client.PATCH(
        f"/admin/v2/project/{proj1_id}/?user_id={new_user_id}"
    )
    assert res.status_code == 200
    # TODO: Add assertion about LinkUserProjectV2 for `old_user_id`

    # Task access
    res = await client.PATCH(
        f"/admin/v2/project/{proj2_id}/?user_id={new_user_id}"
    )
    assert res.status_code == 200
    # Add assertion about what the new user would see (e.g. warnings)

    # Test behavior for a project that had already been shared with new user
    db.add(LinkUserProjectV2(user_id=new_user_id, project_id=proj3_id))
    await db.commit()
    db.expunge_all()
    res = await client.PATCH(
        f"/admin/v2/project/{proj3_id}/?user_id={new_user_id}"
    )
    assert res.status_code == 200
    # TODO: Add assertion about LinkUserProjectV2 for `new_user_id`

    # Wrong resource
    res = await client.PATCH(
        f"/admin/v2/project/{proj4_id}/?user_id={new_user_id}"
    )
    assert res.status_code == 422
