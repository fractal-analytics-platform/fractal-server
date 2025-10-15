async def test_profile_api(
    db,
    client,
    MockCurrentUser,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_db,
):
    local_res, local_prof = local_resource_profile_db
    local_res_id = local_res.id
    local_prof_id = local_prof.id
    slurm_ssh_res, slurm_ssh_prof = slurm_ssh_resource_profile_fake_db
    slurm_ssh_res_id = slurm_ssh_res.id
    slurm_ssh_prof_id = slurm_ssh_prof.id

    async with MockCurrentUser(user_kwargs=dict(is_superuser=True)):
        # GET all profiles of a given resource / failure
        res = await client.get("/admin/v2/resource/9999/profile/")
        assert res.status_code == 404

        # GET all profiles of a given resource / success
        res = await client.get(f"/admin/v2/resource/{local_res_id}/profile/")
        assert res.status_code == 200
        assert len(res.json()) == 1

        # GET a specific profile / failure
        res = await client.get(
            f"/admin/v2/resource/{local_res_id}/profile/{slurm_ssh_prof_id}/"
        )
        assert res.status_code == 404

        # GET a specific profile / success
        res = await client.get(
            f"/admin/v2/resource/{local_res_id}/profile/{local_prof_id}/"
        )
        assert res.status_code == 200

        # POST one profile / success
        res = await client.post(
            f"/admin/v2/resource/{local_res_id}/profile/",
            json=dict(resource_type="local", name="name1"),
        )
        assert res.status_code == 201
        assert res.json()["name"] == "name1"

        # POST one profile / failure due to invalid `resource_type`
        res = await client.post(
            f"/admin/v2/resource/{local_res_id}/profile/",
            json=dict(resource_type="invalid"),
        )
        assert res.status_code == 422
        assert "union_tag_invalid" in str(res.json()["detail"])

        # POST one profile / failure due to name taken
        res = await client.post(
            f"/admin/v2/resource/{slurm_ssh_res_id}/profile/",
            json=dict(name="name1"),
        )
        assert res.status_code == 422
        assert "already exists" in str(res.json()["detail"])

        # PUT one profile / success
        NEW_NAME = "new-name"
        NEW_USERNAME = "new-username"
        new_ssh_profile = slurm_ssh_prof.model_dump()
        new_ssh_profile["name"] = NEW_NAME
        new_ssh_profile["username"] = NEW_USERNAME
        res = await client.put(
            (
                f"/admin/v2/resource/{slurm_ssh_res_id}/"
                f"profile/{slurm_ssh_prof_id}/"
            ),
            json=new_ssh_profile,
        )
        assert res.status_code == 200
        assert res.json()["username"] == NEW_USERNAME
        assert res.json()["name"] == NEW_NAME

        # PUT one profile / failure
        new_ssh_profile["username"] = None
        res = await client.put(
            (
                f"/admin/v2/resource/{slurm_ssh_res_id}/"
                f"profile/{slurm_ssh_prof_id}/"
            ),
            json=new_ssh_profile,
        )
        assert res.status_code == 422
        assert res.json() == {
            "detail": [
                {
                    "type": "string_type",
                    "loc": [
                        "body",
                        "slurm_ssh",
                        "username",
                    ],
                    "msg": "Input should be a valid string",
                    "input": None,
                },
            ],
        }

        # DELETE one profile
        res = await client.delete(
            (
                f"/admin/v2/resource/{slurm_ssh_res_id}/"
                f"profile/{slurm_ssh_prof_id}/"
            ),
        )
        assert res.status_code == 204
        res = await client.get(
            (
                f"/admin/v2/resource/{slurm_ssh_res_id}/"
                f"profile/{slurm_ssh_prof_id}/"
            ),
        )
        assert res.status_code == 404


async def test_resource_of_profile(
    db,
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    async with MockCurrentUser(user_kwargs=dict(is_superuser=True)):
        # Failure
        res = await client.get("/admin/v2/resource-of-profile/9999/")
        assert res.status_code == 404
        # Success
        res = await client.get(f"/admin/v2/resource-of-profile/{profile.id}/")
        assert res.status_code == 200
        assert res.json()["id"] == resource.id
