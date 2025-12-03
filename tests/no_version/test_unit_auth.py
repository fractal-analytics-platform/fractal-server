import pytest
from devtools import debug
from fastapi import HTTPException
from sqlmodel import select

from fractal_server.app.models import DatasetV2
from fractal_server.app.models import LinkUserProjectV2
from fractal_server.app.models import ProjectV2
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth._aux_auth import _check_project_dirs_update
from fractal_server.app.routes.auth._aux_auth import (
    _get_single_user_with_groups,
)
from fractal_server.app.routes.auth._aux_auth import (
    _get_single_usergroup_with_user_ids,
)
from fractal_server.app.routes.auth._aux_auth import _user_or_404
from fractal_server.app.routes.auth._aux_auth import (
    _verify_user_belongs_to_group,
)
from fractal_server.app.schemas.v2 import ProjectPermissions
from fractal_server.app.security import _create_first_user


async def _get_first_user(db):
    stm = select(UserOAuth)
    res = await db.execute(stm)
    return res.scalars().unique().all()[0]


async def test_user_or_404(db):
    with pytest.raises(HTTPException) as exc_info:
        await _user_or_404(user_id=9999, db=db)
    debug(exc_info.value)
    await _create_first_user(
        email="test1@fractal.com",
        password="xxxx",
        project_dir="/fake",
    )
    user = await _get_first_user(db)
    await _user_or_404(user_id=user.id, db=db)


async def test_get_single_group_with_user_ids(db):
    with pytest.raises(HTTPException) as exc_info:
        await _get_single_usergroup_with_user_ids(group_id=9999, db=db)
    debug(exc_info.value)


async def test_get_single_user_with_groups(db):
    await _create_first_user(
        email="test1@fractal.com", password="xxxx", project_dir="/fake"
    )
    user = await _get_first_user(db)
    res = await _get_single_user_with_groups(user=user, db=db)
    debug(res)


async def test_verify_user_belongs_to_group(db):
    with pytest.raises(HTTPException) as exc_info:
        await _verify_user_belongs_to_group(user_id=1, user_group_id=42, db=db)
    debug(exc_info.value)


async def test_check_project_dirs_update(local_resource_profile_db, db):
    # Setup

    resource, _ = local_resource_profile_db
    # Add User
    user = UserOAuth(
        email="user@example.org",
        hashed_password="12345",
        project_dirs=[
            "/example",
            "/foo/bar",  # dataset1
            "/test",
            "/test/data",  # dataset2
            "/test-1",  # dataset3
        ],
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    # Add Project
    project = ProjectV2(name="Project", resource_id=resource.id)
    db.add(project)
    await db.commit()
    await db.refresh(project)
    db.add(
        LinkUserProjectV2(
            project_id=project.id,
            user_id=user.id,
            is_owner=True,
            is_verified=True,
            permissions=ProjectPermissions.EXECUTE,
        )
    )
    await db.commit()
    # Add Datasets
    dataset1 = DatasetV2(
        name="Dataset 3",
        project_id=project.id,
        zarr_dir="/foo/bar/dataset/zarr",
    )
    dataset2 = DatasetV2(
        name="Dataset 1",
        project_id=project.id,
        zarr_dir="/test/data/dataset/zarr",
    )
    dataset3 = DatasetV2(
        name="Dataset 2",
        project_id=project.id,
        zarr_dir="/test-1/dataset/zarr",
    )
    db.add_all([dataset1, dataset2, dataset3])
    await db.commit()
    await db.refresh(dataset1)
    await db.refresh(dataset2)
    await db.refresh(dataset3)

    kwargs = dict(
        old_project_dirs=user.project_dirs,
        user_id=user.id,
        db=db,
    )

    # Test

    # Removing "/example" is OK
    await _check_project_dirs_update(
        new_project_dirs=[
            "/foo/bar",  # dataset1
            "/test",
            "/test/data",  # dataset2
            "/test-1",  # dataset3
        ],
        **kwargs,
    )
    # Removing "/test" is OK
    await _check_project_dirs_update(
        new_project_dirs=[
            "/example",
            "/foo/bar",  # dataset1
            "/test/data",  # dataset2
            "/test-1",  # dataset3
        ],
        **kwargs,
    )
    # Removing both "/example" and "/test" is OK
    await _check_project_dirs_update(
        new_project_dirs=[
            "/foo/bar",  # dataset1
            "/test/data",  # dataset2
            "/test-1",  # dataset3
        ],
        **kwargs,
    )
    # Removing "/foo/bar" can be done after removing dataset1
    with pytest.raises(HTTPException):
        await _check_project_dirs_update(
            new_project_dirs=[
                "/example",
                "/test",
                "/test/data",  # dataset2
                "/test-1",  # dataset3
            ],
            **kwargs,
        )
    await db.delete(dataset1)
    await _check_project_dirs_update(
        new_project_dirs=[
            "/example",
            "/test",
            "/test/data",  # dataset2
            "/test-1",  # dataset3
        ],
        **kwargs,
    )
    # Changing "/test/data" into something more specific is OK
    await _check_project_dirs_update(
        new_project_dirs=[
            "/example",
            "/foo/bar",
            "/test",
            "/test/data/dataset/zarr/",  # dataset2
            "/test-1",  # dataset3
        ],
        **kwargs,
    )
    # Removing "/test/data" is OK, as long as we have "/test"
    await _check_project_dirs_update(
        new_project_dirs=[
            "/example",
            "/foo/bar",
            "/test",  # dataset2
            "/test-1",  # dataset3
        ],
        **kwargs,
    )
    with pytest.raises(HTTPException):
        await _check_project_dirs_update(
            new_project_dirs=[
                "/example",
                "/foo/bar",
                "/test-1",  # dataset3
            ],
            **kwargs,
        )


async def test_check_project_dirs_update_trailing_slash(
    local_resource_profile_db, db
):
    # Setup

    resource, _ = local_resource_profile_db

    # Add User
    user = UserOAuth(
        email="user@example.org",
        hashed_password="12345",
        project_dirs=["/data"],
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    # Add Project
    project = ProjectV2(name="Project", resource_id=resource.id)
    db.add(project)
    await db.commit()
    await db.refresh(project)
    db.add(
        LinkUserProjectV2(
            project_id=project.id,
            user_id=user.id,
            is_owner=True,
            is_verified=True,
            permissions=ProjectPermissions.EXECUTE,
        )
    )
    await db.commit()
    # Add Datasets
    dataset = DatasetV2(
        name="Dataset",
        project_id=project.id,
        zarr_dir="/data/",
    )
    db.add(dataset)
    await db.commit()
    await db.refresh(dataset)

    # Test

    with pytest.raises(HTTPException) as e:
        await _check_project_dirs_update(
            old_project_dirs=user.project_dirs,
            new_project_dirs=[],
            user_id=user.id,
            db=db,
        )
    assert e.status_code == 422
