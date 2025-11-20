import time

from devtools import debug
from sqlalchemy import func
from sqlmodel import select

from fractal_server.app.db import AsyncSession
from fractal_server.app.models import UserOAuth
from fractal_server.app.models.v2 import LinkUserProjectV2
from fractal_server.app.models.v2 import ProjectV2


async def query1(*, user_id: int, db: AsyncSession):
    owner_subquery = (
        select(
            LinkUserProjectV2.project_id, UserOAuth.email.label("owner_email")
        )
        .join(UserOAuth, UserOAuth.id == LinkUserProjectV2.user_id)
        .where(LinkUserProjectV2.is_owner.is_(True))
        .subquery()
    )

    res = await db.execute(
        select(
            ProjectV2.id,
            ProjectV2.name,
            LinkUserProjectV2.permissions,
            owner_subquery.c.owner_email,
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .join(owner_subquery, owner_subquery.c.project_id == ProjectV2.id)
        .where(LinkUserProjectV2.user_id == user_id)
        .where(LinkUserProjectV2.is_verified.is_(False))
        .order_by(ProjectV2.name)
    )
    return res.all()


async def query2(*, user_id: int, db: AsyncSession):
    res = await db.execute(
        select(
            ProjectV2.id,
            ProjectV2.name,
            LinkUserProjectV2.permissions,
            (
                select(UserOAuth.email)
                .join(
                    LinkUserProjectV2,
                    UserOAuth.id == LinkUserProjectV2.user_id,
                )
                .where(LinkUserProjectV2.is_owner.is_(True))
                .where(LinkUserProjectV2.project_id == ProjectV2.id)
                .scalar_subquery()
                .correlate(ProjectV2)
            ),
        )
        .join(LinkUserProjectV2, LinkUserProjectV2.project_id == ProjectV2.id)
        .where(LinkUserProjectV2.user_id == user_id)
        .where(LinkUserProjectV2.is_verified.is_(False))
        .order_by(ProjectV2.name)
    )
    return res.all()


async def compare_queries(*, user_id: int, db: AsyncSession):
    runs = 5

    tot_q1 = 0.0
    for run in range(runs):
        db.expunge_all()
        t_start = time.perf_counter()
        out1 = await query1(db=db, user_id=user_id)
        tot_q1 += time.perf_counter() - t_start

    tot_q2 = 0.0
    for run in range(runs):
        db.expunge_all()
        t_start = time.perf_counter()
        out2 = await query2(db=db, user_id=user_id)
        tot_q2 += time.perf_counter() - t_start

    assert out1 == out2

    av_q1 = tot_q1 / runs
    av_q2 = tot_q2 / runs
    print(f"Query 1: {av_q1:.4f} s")
    print(f"Query 2: {av_q2:.4f} s")


async def test_compare_queries(
    client,
    db: AsyncSession,
    MockCurrentUser,
    local_resource_profile_db,
):
    resource, profile = local_resource_profile_db

    N_OWNERS = 400

    args = dict(
        hashed_password="12345",
        project_dir="/fake",
        is_verified=True,
        profile_id=profile.id,
    )
    guest = UserOAuth(email="guest@example.org", **args)
    db.add(guest)
    await db.commit()
    await db.refresh(guest)

    current_users = []
    for ind in range(N_OWNERS):
        user = UserOAuth(email=f"{ind}@example.org", **args)
        db.add(user)
        await db.flush()
        current_users.append(user)
        project = ProjectV2(name=f"Project {ind}", resource_id=resource.id)
        db.add(project)
        await db.flush()
        db.add(
            LinkUserProjectV2(
                user_id=user.id,
                project_id=project.id,
                is_owner=True,
                is_verified=True,
                permissions="rwx",
            )
        )
        db.add(
            LinkUserProjectV2(
                user_id=guest.id,
                project_id=project.id,
                is_owner=False,
                is_verified=False,
                permissions="r",
            )
        )
        for _other_user in current_users[:-1]:
            db.add(
                LinkUserProjectV2(
                    user_id=_other_user.id,
                    project_id=project.id,
                    is_owner=False,
                    is_verified=False,
                    permissions="r",
                )
            )
    await db.commit()

    num_links = (
        await db.execute(select(func.count(LinkUserProjectV2.user_id)))
    ).scalar()
    num_links_guest = (
        await db.execute(
            select(func.count(LinkUserProjectV2.user_id)).where(
                LinkUserProjectV2.user_id == guest.id
            )
        )
    ).scalar()
    print()
    print(f"{num_links=}")
    print(f"{num_links_guest=}")
    print()

    async with MockCurrentUser(user_kwargs={"id": guest.id}):
        # Get list of invitations
        res = await client.get("/api/v2/project/invitation/")
        assert res.status_code == 200
        if N_OWNERS < 5:
            debug(res.json())

        await compare_queries(db=db, user_id=guest.id)
