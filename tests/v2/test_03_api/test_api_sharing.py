from pathlib import Path

from fractal_server.app.db import AsyncSession
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.schemas.v2 import ProjectPermissions


async def test_project_sharing(
    client,
    db: AsyncSession,
    MockCurrentUser,
    local_resource_profile_db,
    tmp_path: Path,
):
    resource, profile = local_resource_profile_db

    # Create 3 users
    args = dict(
        hashed_password="12345",
        project_dir=tmp_path.as_posix(),
        is_verified=True,
        profile_id=profile.id,
    )
    user1 = UserOAuth(email="user1@test.org", **args)
    user2 = UserOAuth(email="user2@test.org", **args)
    user3 = UserOAuth(email="user3@test.org", **args)
    user4 = UserOAuth(email="user4@test.org", **args)
    db.add_all([user1, user2, user3, user4])
    await db.commit()
    await db.refresh(user1)
    await db.refresh(user2)
    await db.refresh(user3)
    await db.refresh(user4)

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Create Project1
        res = await client.post("/api/v2/project/", json=dict(name="Project1"))
        assert res.status_code == 201
        project1 = res.json()
        project1_id = project1["id"]

        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == []

        # Invite User2
        res = await client.post(
            f"/api/v2/project/{project1_id}/link/?email={user2.email}",
            json=dict(permissions=ProjectPermissions.READ),
        )
        assert res.status_code == 201

        # Invite User3
        res = await client.post(
            f"/api/v2/project/{project1_id}/link/?email={user3.email}",
            json=dict(permissions=ProjectPermissions.WRITE),
        )
        assert res.status_code == 201

        # Invite User4
        res = await client.post(
            f"/api/v2/project/{project1_id}/link/?email={user4.email}",
            json=dict(permissions=ProjectPermissions.EXECUTE),
        )
        assert res.status_code == 201

        # Get the same list as above
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                guest_email=user2.email,
                is_verified=False,
                permissions=ProjectPermissions.READ,
            ),
            dict(
                guest_email=user3.email,
                is_verified=False,
                permissions=ProjectPermissions.WRITE,
            ),
            dict(
                guest_email=user4.email,
                is_verified=False,
                permissions=ProjectPermissions.EXECUTE,
            ),
        ]

    async with MockCurrentUser(user_kwargs={"id": user2.id}):
        # Get list of projects
        res = await client.get("/api/v2/project/")
        assert res.json() == []
        res = await client.get("/api/v2/project/?is_owner=false")
        assert res.json() == []

        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert res.status_code == 200
        assert res.json() == [
            dict(
                project_name=project1["name"],
                project_id=project1_id,
                owner_email=user1.email,
                permissions=ProjectPermissions.READ,
            )
        ]

        # Accept invitation
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest-link/accept/",
        )
        assert res.status_code == 200

        # Get list of projects
        res = await client.get("/api/v2/project/")
        assert res.json() == []
        res = await client.get("/api/v2/project/?is_owner=false")
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == project1_id

        # Create Project2
        res = await client.post("/api/v2/project/", json=dict(name="Project3"))
        assert res.status_code == 201
        project2_id = res.json()["id"]

        # Get list of projects
        res = await client.get("/api/v2/project/")
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == project2_id
        res = await client.get("/api/v2/project/?is_owner=false")
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == project1_id

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                guest_email=user2.email,
                is_verified=True,
                permissions=ProjectPermissions.READ,
            ),
            dict(
                guest_email=user3.email,
                is_verified=False,
                permissions=ProjectPermissions.WRITE,
            ),
            dict(
                guest_email=user4.email,
                is_verified=False,
                permissions=ProjectPermissions.EXECUTE,
            ),
        ]

    async with MockCurrentUser(user_kwargs={"id": user3.id}):
        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert res.status_code == 200
        assert res.json() == [
            dict(
                project_name=project1["name"],
                project_id=project1_id,
                owner_email=user1.email,
                permissions=ProjectPermissions.WRITE,
            )
        ]

        # Reject invitation
        res = await client.delete(
            f"/api/v2/project/{project1_id}/guest-link/",
        )
        assert res.status_code == 204

        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert res.status_code == 200
        assert res.json() == []

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                guest_email=user2.email,
                is_verified=True,
                permissions=ProjectPermissions.READ,
            ),
            dict(
                guest_email=user4.email,
                is_verified=False,
                permissions=ProjectPermissions.EXECUTE,
            ),
        ]

    async with MockCurrentUser(user_kwargs={"id": user2.id}):
        # Exit project
        res = await client.delete(
            f"/api/v2/project/{project1_id}/guest-link/",
        )
        assert res.status_code == 204

    async with MockCurrentUser(user_kwargs={"id": user4.id}):
        # Accept invitation
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest-link/accept/",
        )
        assert res.status_code == 200

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                guest_email=user4.email,
                is_verified=True,
                permissions=ProjectPermissions.EXECUTE,
            ),
        ]

        # Change permissions of User4
        res = await client.patch(
            f"/api/v2/project/{project1_id}/link/?email={user4.email}",
            json=dict(permissions=ProjectPermissions.WRITE),
        )
        assert res.status_code == 200

        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                guest_email=user4.email,
                is_verified=True,
                permissions=ProjectPermissions.WRITE,
            ),
        ]

        # Kick out User4
        res = await client.delete(
            f"/api/v2/project/{project1_id}/link/?email={user4.email}",
        )
        assert res.status_code == 204

        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == []
