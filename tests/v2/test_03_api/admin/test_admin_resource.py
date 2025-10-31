import pytest
from fastapi import HTTPException

from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.app.routes.admin.v2.resource import (
    _check_resource_type_match_or_422,
)


def test_check_resource_type_match_or_422(
    local_resource_profile_objects,
):
    resource, old_profile = local_resource_profile_objects[:]
    new_profile_ok = Profile(**old_profile.model_dump())
    new_profile_bad = Profile(
        **old_profile.model_dump(exclude={"resource_type"}),
        resource_type="slurm_ssh",
    )

    _check_resource_type_match_or_422(
        resource=resource,
        new_profile=new_profile_ok,
    )
    with pytest.raises(HTTPException, match="differs"):
        _check_resource_type_match_or_422(
            resource=resource,
            new_profile=new_profile_bad,
        )


async def test_resource_api(
    db,
    client,
    MockCurrentUser,
    local_resource_profile_db,
    slurm_ssh_resource_profile_fake_objects,
):
    async with MockCurrentUser(
        user_kwargs=dict(is_superuser=True)
    ) as superuser:
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
        assert (
            'key constraint "profile_resource_id_fkey"' in res.json()["detail"]
        )
        res = await client.delete(f"/admin/v2/profile/{profile['id']}/")
        assert res.status_code == 204

        project = ProjectV2(name="project", resource_id=resource_id)
        db.add(project)
        await db.commit()
        await db.refresh(project)
        res = await client.delete(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 422
        assert (
            'key constraint "projectv2_resource_id_fkey"'
            in res.json()["detail"]
        )
        await db.delete(project)
        await db.commit()

        task_group = TaskGroupV2(
            user_id=superuser.id,
            origin="other",
            pkg_name="pkg",
            resource_id=resource_id,
        )
        db.add(task_group)
        await db.commit()
        await db.refresh(task_group)
        res = await client.delete(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 422
        assert (
            'key constraint "taskgroupv2_resource_id_fkey"'
            in res.json()["detail"]
        )
        await db.delete(task_group)
        await db.commit()

        # DELETE one resource / success
        res = await client.delete(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 204
        res = await client.get(f"/admin/v2/resource/{resource_id}/")
        assert res.status_code == 404
