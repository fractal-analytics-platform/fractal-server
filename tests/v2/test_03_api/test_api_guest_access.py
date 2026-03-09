async def test_api_guest_access(
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    _, profile = local_resource_profile_db

    async with MockCurrentUser(profile_id=profile.id, is_guest=True):
        res = await client.get("/api/v2/project/")
        assert res.status_code == 200

        res = await client.post("/api/v2/project/", json=dict(name="project"))
        assert res.status_code == 403
        assert res.json()["detail"] == (
            "This feature is not available for guest users."
        )

    async with MockCurrentUser(profile_id=profile.id):
        res = await client.get("/api/v2/project/")
        assert res.status_code == 200

        res = await client.post("/api/v2/project/", json=dict(name="project"))
        assert res.status_code == 201
