from pathlib import Path

from devtools import debug

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
    db.add_all([user1, user2, user3])
    await db.commit()
    await db.refresh(user1)
    await db.refresh(user2)
    await db.refresh(user3)

    async with MockCurrentUser(user_kwargs={"id": user1.id}):
        # Create Project1
        res = await client.post("/api/v2/project/", json=dict(name="Project1"))
        assert res.status_code == 201
        project1_id = res.json()["id"]

        # Get list of all users linked to Project1
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == []

        # Invite User2
        res = await client.post(
            f"/api/v2/project/{project1_id}/link/?email={user2.email}", json={}
        )
        debug(res.json())
        assert res.status_code == 201

        # Get the same list as above
        res = await client.get(
            f"/api/v2/project/{project1_id}/link/",
        )
        assert res.status_code == 200
        assert res.json() == [
            dict(
                user_email=user2.email,
                is_verified=False,
                permissions=ProjectPermissions.READ,
            )
        ]
