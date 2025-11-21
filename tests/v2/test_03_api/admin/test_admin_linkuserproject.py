from devtools import debug

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models.security import UserOAuth
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.schemas.v2 import ProjectPermissions

PREFIX = "/admin/v2"


async def test_view_link_user_project(
    db: AsyncSession,
    client,
    MockCurrentUser,
    local_resource_profile_db,
):
    N = 11

    resource, profile = local_resource_profile_db

    # Create N users, each with one project
    db.add_all(
        [
            UserOAuth(
                id=i,
                email=f"user{i}@example.org",
                hashed_password="12345",
                project_dir="/fake",
                is_verified=True,
                profile_id=profile.id,
            )
            for i in range(N)
        ]
    )
    await db.flush()
    db.add_all(
        [
            ProjectV2(
                id=i,
                name=[
                    f"projectA{i}",
                    f"projectB{i}",
                ][i % 2],
                resource_id=resource.id,
            )
            for i in range(N)
        ]
    )
    await db.flush()
    db.add_all(
        [
            LinkUserProjectV2(
                project_id=i,
                user_id=i,
                is_owner=True,
                is_verified=True,
                permissions=ProjectPermissions.EXECUTE,
            )
            for i in range(N)
        ]
    )
    await db.commit()

    # Invite every user to every project
    db.add_all(
        [
            LinkUserProjectV2(
                project_id=i,
                user_id=j,
                is_owner=False,
                is_verified=[
                    True,
                    False,
                ][i % 2],
                permissions=[
                    ProjectPermissions.READ,
                    ProjectPermissions.WRITE,
                    ProjectPermissions.EXECUTE,
                ][i % 3],
            )
            for i in range(N)
            for j in range(N)
            if i != j
        ]
    )
    await db.commit()

    superuser = UserOAuth(
        id=N + 1,
        email="admin@example.org",
        hashed_password="12345",
        project_dir="/fake",
        is_verified=True,
        is_superuser=True,
        profile_id=profile.id,
    )
    db.add(superuser)
    await db.commit()
    await db.refresh(superuser)

    async with MockCurrentUser(user_kwargs={"id": superuser.id}):
        # page_size
        res = await client.get("/admin/v2/linkuserproject/?page_size=3")
        assert res.status_code == 200
        assert res.json()["page_size"] == 3
        assert res.json()["total_count"] == N**2

        # project_id
        res = await client.get("/admin/v2/linkuserproject/?project_id=2")
        assert res.status_code == 200
        assert res.json()["page_size"] == res.json()["total_count"] == N
        assert all((item["project_id"] == 2 for item in res.json()["items"]))

        # is_owner
        res = await client.get(
            "/admin/v2/linkuserproject/?project_id=2&is_owner=false"
        )
        assert res.status_code == 200
        assert res.json()["page_size"] == res.json()["total_count"] == N - 1
        res = await client.get("/admin/v2/linkuserproject/?is_owner=true")
        assert res.status_code == 200
        assert res.json()["page_size"] == res.json()["total_count"] == N

        # project_name
        res = await client.get("/admin/v2/linkuserproject/?project_name=b")
        assert res.status_code == 200
        # Only half of the projects i-contain the char 'b'
        # (if `?project_name=a` the total count is `(N//2 + N%2) * N`).
        assert res.json()["total_count"] == N // 2 * N
        assert all(
            (
                item["project_name"].startswith("projectB")
                for item in res.json()["items"]
            )
        )

        # user_id & is_verified
        res = await client.get(
            "/admin/v2/linkuserproject/?user_id=1&is_verified=true"
        )
        assert res.status_code == 200
        assert (
            len(set((item["user_email"] for item in res.json()["items"]))) == 1
        )  # only one user_email
        debug(res.json())
        assert all((item["is_verified"] for item in res.json()["items"]))
