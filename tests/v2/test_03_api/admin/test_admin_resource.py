import pytest

from fractal_server.app.schemas.v2.resource import validate_resource


def test_validate_resource():
    with pytest.raises(
        ValueError,
        match="Missing `type` key",
    ):
        validate_resource({})


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
        faulty_slurm_ssh_resource = slurm_ssh_resource_profile_fake_objects[
            0
        ].model_dump(exclude={"timestamp_created", "id"})
        FAULTY_RESOURCE_EXPECTED_ERROR = {
            "detail": [
                {
                    "type": "string_type",
                    "loc": [
                        "body",
                        "slurm_ssh",
                        "host",
                    ],
                    "msg": "Input should be a valid string",
                    "input": None,
                },
            ],
        }
        faulty_slurm_ssh_resource["host"] = None
        res = await client.post(
            "/admin/v2/resource/",
            json=faulty_slurm_ssh_resource,
        )
        assert res.status_code == 422
        assert res.json() == FAULTY_RESOURCE_EXPECTED_ERROR
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
                exclude={"timestamp_created", "id"},
            ),
        )
        assert res.status_code == 422
        assert "already exists" in str(res.json()["detail"])

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

        # PUT one resource / missing `type`
        res = await client.put(
            f"/admin/v2/resource/{resource_id}/",
            json={},
        )
        assert res.status_code == 422
        assert "union_tag_not_found" in str(res.json()["detail"])

        # PUT one resource / failure due to invalid request body
        res = await client.put(
            f"/admin/v2/resource/{resource_id}/",
            json=faulty_slurm_ssh_resource,
        )
        assert res.status_code == 422
        assert res.json() == FAULTY_RESOURCE_EXPECTED_ERROR

        # PUT one resource / failure due to non-unique name
        valid_new_resource = local_resource_profile_db[0].model_dump(
            exclude={
                "timestamp_created",
                "id",
            }
        )
        valid_new_resource["name"] = local_resource_profile_db[0].name
        res = await client.put(
            f"/admin/v2/resource/{resource_id}/",
            json=valid_new_resource,
        )
        assert res.status_code == 422
        assert "already exists" in str(res.json()["detail"])

        # PUT one resource / success
        NEW_NAME = "A new name"
        valid_new_resource["name"] = NEW_NAME
        res = await client.put(
            f"/admin/v2/resource/{resource_id}/", json=valid_new_resource
        )
        assert res.status_code == 200
        assert res.json()["name"] == NEW_NAME

        # DELETE one resource / failure
        res = await client.post(
            f"/admin/v2/resource/{resource_id}/profile/",
            json=dict(resource_type="local", name="name"),
        )
        assert res.status_code == 201
        profile = res.json()
        res = await client.delete(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 422
        assert "it's associated with 1 Profiles" in str(res.json()["detail"])

        # DELETE one resource / success
        res = await client.delete(
            f"/admin/v2/resource/{resource_id}/profile/{profile['id']}/"
        )
        assert res.status_code == 204
        res = await client.delete(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 204
        res = await client.get(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 404
