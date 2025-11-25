from fractal_server.app.db import AsyncSession
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.schemas.v2 import ProjectPermissions


async def test_project_sharing(
    client,
    db: AsyncSession,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db

    # Create 3 users
    args = dict(
        hashed_password="12345",
        project_dir="/fake",
        is_verified=True,
        profile_id=profile.id,
    )
    user1 = UserOAuth(email="zzz@example.org", **args)
    user2 = UserOAuth(email="yyy@example.org", **args)
    user3 = UserOAuth(email="xxx@example.org", **args)
    user4 = UserOAuth(email="www@example.org", **args)
    db.add_all([user1, user2, user3, user4])
    await db.commit()
    await db.refresh(user1)
    await db.refresh(user2)
    await db.refresh(user3)
    await db.refresh(user4)

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Create Project1
        res = await client.post("/api/v2/project/", json=dict(name="ProjectZ"))
        assert res.status_code == 201
        project1 = res.json()
        project1_id = project1["id"]

        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/guest/",
        )
        assert res.status_code == 200
        assert res.json() == []

        # Check User1 access to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/access/",
        )
        assert res.status_code == 200
        assert res.json() == dict(
            is_owner=True,
            permissions=ProjectPermissions.EXECUTE,
            owner_email=user1.email,
        )

        # Invite User2
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest/?email={user2.email}",
            json=dict(permissions=ProjectPermissions.READ),
        )
        assert res.status_code == 201

        # Invite User3
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest/?email={user3.email}",
            json=dict(permissions=ProjectPermissions.WRITE),
        )
        assert res.status_code == 201

        # Invite User4
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest/?email={user4.email}",
            json=dict(permissions=ProjectPermissions.EXECUTE),
        )
        assert res.status_code == 201

        # Get the same list as above
        res = await client.get(
            f"/api/v2/project/{project1_id}/guest/",
        )
        assert res.status_code == 200
        assert res.json() == sorted(
            [
                dict(
                    email=user2.email,
                    is_verified=False,
                    permissions=ProjectPermissions.READ,
                ),
                dict(
                    email=user3.email,
                    is_verified=False,
                    permissions=ProjectPermissions.WRITE,
                ),
                dict(
                    email=user4.email,
                    is_verified=False,
                    permissions=ProjectPermissions.EXECUTE,
                ),
            ],
            key=lambda item: item["email"],
        )

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
                guest_permissions=ProjectPermissions.READ,
            )
        ]

        # Check User2 access to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/access/",
        )
        assert res.status_code == 404
        assert res.json()["detail"] == (
            f"User has no access to project {project1_id}."
        )

        # Accept invitation
        res = await client.post(
            f"/api/v2/project/{project1_id}/access/accept/",
        )
        assert res.status_code == 200

        # Get list of projects
        res = await client.get("/api/v2/project/")
        assert res.json() == []
        res = await client.get("/api/v2/project/?is_owner=false")
        assert len(res.json()) == 1
        assert res.json()[0]["id"] == project1_id

        # Check User2 access to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/access/",
        )
        assert res.status_code == 200
        assert res.json() == dict(
            is_owner=False,
            permissions=ProjectPermissions.READ,
            owner_email=user1.email,
        )

        # Create Project2
        res = await client.post("/api/v2/project/", json=dict(name="ProjectY"))
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
            f"/api/v2/project/{project1_id}/guest/",
        )
        assert res.status_code == 200
        assert res.json() == sorted(
            [
                dict(
                    email=user2.email,
                    is_verified=True,
                    permissions=ProjectPermissions.READ,
                ),
                dict(
                    email=user3.email,
                    is_verified=False,
                    permissions=ProjectPermissions.WRITE,
                ),
                dict(
                    email=user4.email,
                    is_verified=False,
                    permissions=ProjectPermissions.EXECUTE,
                ),
            ],
            key=lambda item: item["email"],
        )

    async with MockCurrentUser(user_kwargs={"id": user3.id}):
        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert res.status_code == 200
        assert res.json() == [
            dict(
                project_name=project1["name"],
                project_id=project1_id,
                owner_email=user1.email,
                guest_permissions=ProjectPermissions.WRITE,
            )
        ]

        # Reject invitation
        res = await client.delete(
            f"/api/v2/project/{project1_id}/access/",
        )
        assert res.status_code == 204

        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert res.status_code == 200
        assert res.json() == []

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/guest/",
        )
        assert res.status_code == 200
        assert res.json() == sorted(
            [
                dict(
                    email=user2.email,
                    is_verified=True,
                    permissions=ProjectPermissions.READ,
                ),
                dict(
                    email=user4.email,
                    is_verified=False,
                    permissions=ProjectPermissions.EXECUTE,
                ),
            ],
            key=lambda item: item["email"],
        )

    async with MockCurrentUser(user_kwargs={"id": user2.id}):
        # Exit project
        res = await client.delete(
            f"/api/v2/project/{project1_id}/access/",
        )
        assert res.status_code == 204

    async with MockCurrentUser(user_kwargs={"id": user4.id}):
        # Accept invitation
        res = await client.post(
            f"/api/v2/project/{project1_id}/access/accept/",
        )
        assert res.status_code == 200

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/guest/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                email=user4.email,
                is_verified=True,
                permissions=ProjectPermissions.EXECUTE,
            ),
        ]

        # Change permissions of User4
        res = await client.patch(
            f"/api/v2/project/{project1_id}/guest/?email={user4.email}",
            json=dict(permissions=ProjectPermissions.WRITE),
        )
        assert res.status_code == 200

        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/guest/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                email=user4.email,
                is_verified=True,
                permissions=ProjectPermissions.WRITE,
            ),
        ]

        # Kick out User4
        res = await client.delete(
            f"/api/v2/project/{project1_id}/guest/?email={user4.email}",
        )
        assert res.status_code == 204

        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/guest/",
        )
        assert res.status_code == 200
        assert res.json() == []

    # From now on: TESTING FAILURES

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Not project owner
        res = await client.get(
            f"/api/v2/project/{project2_id}/guest/",
        )
        assert res.status_code == 403
        assert res.json()["detail"] == "Current user is not the project owner."

        # Not existing email
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest/?email=foo@example.org",
            json=dict(permissions=ProjectPermissions.READ),
        )
        assert res.status_code == 404
        assert res.json()["detail"] == "User not found."

        # Link already exists
        res = await client.post(
            f"/api/v2/project/{project1_id}/guest/?email={user1.email}",
            json=dict(permissions=ProjectPermissions.READ),
        )
        assert res.status_code == 422
        assert res.json()["detail"] == "User is already associated to project."

        # Link already exists
        res = await client.patch(
            f"/api/v2/project/{project1_id}/guest/?email={user1.email}",
            json=dict(permissions="rw"),
        )
        assert res.status_code == 422
        assert (
            res.json()["detail"]
            == "Cannot perform this operation on project owner."
        )

        # Link not existing
        res = await client.patch(
            f"/api/v2/project/{project1_id}/guest/?email={user4.email}",
            json=dict(permissions="r"),
        )
        assert res.status_code == 404
        assert res.json()["detail"] == "User is not linked to project."

        # Revoke access to owner
        res = await client.delete(
            f"/api/v2/project/{project1_id}/guest/?email={user1.email}",
        )
        assert res.status_code == 422
        assert (
            res.json()["detail"]
            == "Cannot perform this operation on project owner."
        )

        # No pending invitation
        res = await client.post(
            f"/api/v2/project/{project1_id}/access/accept/",
        )
        assert res.status_code == 404
        assert (
            res.json()["detail"]
            == "No pending invitation for user on this project."
        )

        # Owner cannot unsubscribe
        res = await client.delete(
            f"/api/v2/project/{project1_id}/access/",
        )
        assert res.status_code == 422
        assert (
            res.json()["detail"]
            == f"You are the owner of project {project1_id}."
        )


async def test_project_sharing_access_control(
    client,
    db: AsyncSession,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db
    user_args = dict(
        hashed_password="12345",
        project_dir="/fake",
        is_verified=True,
        profile_id=profile.id,
    )
    user1 = UserOAuth(email="zzz@example.org", **user_args)
    user2 = UserOAuth(email="yyy@example.org", **user_args)
    db.add_all([user1, user2])
    await db.commit()
    await db.refresh(user1)
    await db.refresh(user2)

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # User 1 creates the project
        res = await client.post("/api/v2/project/", json=dict(name="Project1"))
        assert res.status_code == 201
        project = res.json()
        project_id = project["id"]

        # and shares it with user 2 with permissions R
        res = await client.post(
            f"/api/v2/project/{project_id}/guest/?email={user2.email}",
            json=dict(permissions=ProjectPermissions.READ),
        )
        assert res.status_code == 201

    async with MockCurrentUser(user_kwargs={"id": user2.id}):
        res = await client.post(
            f"/api/v2/project/{project_id}/access/accept/",
        )
        assert res.status_code == 200

        # User 2 can read the project
        res = await client.get(f"/api/v2/project/{project_id}/")
        assert res.status_code == 200
        assert res.json() == project

        # but not patch it
        res = await client.patch(
            f"/api/v2/project/{project_id}/",
            json=dict(name="new_name"),
        )
        assert res.status_code == 403

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # # User 1 edits permissions into RWX
        res = await client.patch(
            f"/api/v2/project/{project_id}/guest/?email={user2.email}",
            json=dict(permissions=ProjectPermissions.EXECUTE),
        )
        assert res.status_code == 200

    async with MockCurrentUser(user_kwargs={"id": user2.id}):
        # User 2 can read and patch the project
        res = await client.get(f"/api/v2/project/{project_id}/")
        assert res.status_code == 200
        assert res.json() == project

        res = await client.patch(
            f"/api/v2/project/{project_id}/",
            json=dict(name="new_name"),
        )
        assert res.status_code == 200

        # but not delete it
        res = await client.delete(f"/api/v2/project/{project_id}/")
        assert res.status_code == 403
        assert res.json()["detail"] == "Only the owner can delete a Project."

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        res = await client.delete(f"/api/v2/project/{project_id}/")
        assert res.status_code == 204
