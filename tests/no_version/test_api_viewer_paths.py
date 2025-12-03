from fractal_server.app.models.linkuserproject import LinkUserProjectV2
from fractal_server.app.schemas.v2.sharing import ProjectPermissions


async def test_get_current_user_allowed_viewer_paths(
    MockCurrentUser,
    client,
    db,
    local_resource_profile_db,
):
    async with MockCurrentUser(
        user_kwargs=dict(
            profile_id=local_resource_profile_db[1].id,
            project_dirs=["/a1", "/a2"],
        )
    ) as userA:
        # UserA creates a Project with three Datasets:
        res = await client.post("/api/v2/project/", json=dict(name="Project"))
        assert res.status_code == 201
        project_id = res.json()["id"]

        # Dataset1.zatt_dir == '/a1/x'
        res = await client.post(
            f"api/v2/project/{project_id}/dataset/",
            json=dict(
                name="Dataset1",
                project_dir=userA.project_dirs[0],
                zarr_subfolder="x",
            ),
        )
        assert res.status_code == 201

        # Dataset2.zatt_dir == '/a2/y'
        res = await client.post(
            f"api/v2/project/{project_id}/dataset/",
            json=dict(
                name="Dataset2",
                project_dir=userA.project_dirs[1],
                zarr_subfolder="y",
            ),
        )
        assert res.status_code == 201

        # Dataset3.zatt_dir == '/a2/y'
        res = await client.post(
            f"api/v2/project/{project_id}/dataset/",
            json=dict(
                name="Dataset3",
                project_dir=userA.project_dirs[1],
                zarr_subfolder="y",
            ),
        )
        assert res.status_code == 201

        res = await client.get("/auth/current-user/allowed-viewer-paths/")
        assert res.json() == ["/a1", "/a2"]

    async with MockCurrentUser(
        user_kwargs=dict(
            profile_id=local_resource_profile_db[1].id,
            project_dirs=["/b1", "/a1/x", "/b2"],
        )
    ) as userB:
        res = await client.get("/auth/current-user/allowed-viewer-paths/")
        assert res.json() == ["/b1", "/a1/x", "/b2"]

        # UserB becomes guest of the Project
        db.add(
            LinkUserProjectV2(
                project_id=project_id,
                user_id=userB.id,
                is_owner=False,
                is_verified=True,
                permissions=ProjectPermissions.READ,
            )
        )
        await db.commit()

        res = await client.get("/auth/current-user/allowed-viewer-paths/")
        assert res.json() == ["/b1", "/a1/x", "/b2", "/a1/x", "/a2/y"]

        res = await client.get(
            "/auth/current-user/allowed-viewer-paths/"
            "?include_shared_projects=false"
        )
        assert res.json() == ["/b1", "/a1/x", "/b2"]
