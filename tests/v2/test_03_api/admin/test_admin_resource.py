async def test_resource_api(
    db,
    client,
    MockCurrentUser,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_objects,
):
    async with MockCurrentUser(user_kwargs=dict(is_superuser=True)):
        # GET all resources
        res = await client.get("/admin/v2/resource/")
        assert res.status_code == 200
        assert len(res.json()) == 1

        # POST one resource / fail due to invalid payload
        res = await client.post(
            "/admin/v2/resource/",
            json=dict(something="else"),
        )
        assert res.status_code == 422
        assert "Field required" in str(res.json()["detail"])

        # POST one resource / fail due to wrong resource.type
        res = await client.post(
            "/admin/v2/resource/",
            json=slurm_ssh_resource_profile_fake_objects[0].model_dump(
                exclude={"timestamp_created", "id"}
            ),
        )
        assert res.status_code == 422
        assert "FRACTAL_RUNNER_BACKEND" in str(res.json()["detail"])

        # POST one resource / fail due to non-unique name
        res = await client.post(
            "/admin/v2/resource/",
            json=local_resource_profile_db[0].model_dump(
                exclude={"timestamp_created"},
            ),
        )
        assert res.status_code == 422
        assert "already in use" in str(res.json()["detail"])

        # POST one resource / success
        valid_resource = local_resource_profile_db[0].model_dump(
            exclude={
                "timestamp_created",
                "name",
                "id",
            }
        )
        NAME = "another resource name"
        valid_resource["name"] = NAME
        res = await client.post(
            "/admin/v2/resource/",
            json=valid_resource,
        )
        assert res.status_code == 201
        assert res.json()["name"] == NAME
        resource_id = res.json()["id"]

        # GET one resource / success
        res = await client.get(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 200
        assert res.json()["name"] == NAME

        # GET one resource / failure
        res = await client.get("/admin/v2/resource/9999/")
        assert res.status_code == 404

        # PATCH one resource / failure due to extra
        res = await client.patch(
            f"/admin/v2/resource/{resource_id}/",
            json=dict(invalid_extra_key="value"),
        )
        assert res.status_code == 422
        assert "Extra inputs are not permitted" in str(res.json()["detail"])

        # PATCH one resource / failure due to invalid request body
        res = await client.patch(
            f"/admin/v2/resource/{resource_id}/",
            json=dict(name=""),
        )
        assert res.status_code == 422
        assert "string_too_short" in str(res.json()["detail"])

        # PATCH one resource / failure due to invalid fields
        res = await client.patch(
            f"/admin/v2/resource/{resource_id}/",
            json=dict(tasks_python_config=dict(invalid="value")),
        )
        assert res.status_code == 422
        assert "PATCH would lead to invalid resource" in str(
            res.json()["detail"]
        )

        # PATCH one resource / failure due to non-unique name
        NEW_NAME = "something else"
        res = await client.patch(
            f"/admin/v2/resource/{resource_id}/",
            json=dict(name=local_resource_profile_db[0].name),
        )
        assert res.status_code == 422
        assert "already in use" in str(res.json()["detail"])

        # PATCH one resource / success
        NEW_NAME = "something else"
        res = await client.patch(
            f"/admin/v2/resource/{resource_id}/",
            json=dict(name=NEW_NAME),
        )
        assert res.status_code == 200
        assert res.json()["name"] == NEW_NAME

        # DELETE one resource / success
        res = await client.delete(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 204
        res = await client.get(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 404
