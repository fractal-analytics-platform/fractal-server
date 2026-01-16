from devtools import debug

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.schemas.v2 import ProjectPermissions


async def test_project_sharing(
    client,
    db: AsyncSession,
    MockCurrentUser,
    local_resource_profile_db,
):
    _, profile = local_resource_profile_db

    guest_user = UserOAuth(
        email="guest@example.org",
        hashed_password="xx",
        project_dirs=["/fake"],
        is_verified=True,
        is_guest=True,
        profile_id=profile.id,
    )
    standard_user = UserOAuth(
        email="standard@example.org",
        hashed_password="xx",
        project_dirs=["/fake"],
        is_verified=True,
        is_guest=False,
        profile_id=profile.id,
    )
    db.add(standard_user)
    db.add(guest_user)
    await db.commit()
    await db.refresh(guest_user)
    await db.refresh(standard_user)
    db.expunge_all()

    async with MockCurrentUser(profile_id=profile.id):
        # Create projects
        res = await client.post("/api/v2/project/", json=dict(name="proj1"))
        project1_id = res.json()["id"]
        res = await client.post("/api/v2/project/", json=dict(name="proj2"))
        project2_id = res.json()["id"]
        # Invite guest and standard users
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest/?email={guest_user.email}",
            json=dict(permissions=ProjectPermissions.READ),
        )
        assert res.status_code == 201
        res = await client.post(
            f"/api/v2/project/{project2_id}/guest/?email={guest_user.email}",
            json=dict(permissions=ProjectPermissions.WRITE),
        )
        assert res.status_code == 201
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest/?email={standard_user.email}",
            json=dict(permissions=ProjectPermissions.READ),
        )
        assert res.status_code == 201

    async with MockCurrentUser(user_id=guest_user.id):
        # List projects
        res = await client.get("/api/v2/project/?is_owner=false")
        assert res.json() == []
        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert len(res.json()) == 2
        # Accept invitation --> forbidden
        res = await client.post(
            f"/api/v2/project/{project1_id}/access/accept/",
        )
        assert res.status_code == 403

    async with MockCurrentUser(is_superuser=True):
        # Accept invitation for guest user with READ permissions
        res = await client.post(
            (
                f"/admin/v2/linkuserproject/verify/"
                f"?project_id={project1_id}&guest_user_id={guest_user.id}"
            )
        )
        assert res.status_code == 200
        # Accept invitation for guest user with WRITE permissions
        res = await client.post(
            (
                f"/admin/v2/linkuserproject/verify/"
                f"?project_id={project2_id}&guest_user_id={guest_user.id}"
            )
        )
        assert res.status_code == 422
        debug(res.json())
        # Accept invitation for standard user --> forbidden
        res = await client.post(
            (
                f"/admin/v2/linkuserproject/verify/"
                f"?project_id={project1_id}&guest_user_id={standard_user.id}"
            )
        )
        assert res.status_code == 422
        debug(res.json())
        # Accept invitation that does not exist -> 404
        res = await client.post(
            (
                f"/admin/v2/linkuserproject/verify/"
                f"?project_id=12345&guest_user_id={guest_user.id}"
            )
        )
        assert res.status_code == 404
        debug(res.json())

    async with MockCurrentUser(user_id=guest_user.id):
        # List projects
        res = await client.get("/api/v2/project/?is_owner=false")
        assert len(res.json()) == 1
        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert len(res.json()) == 1
