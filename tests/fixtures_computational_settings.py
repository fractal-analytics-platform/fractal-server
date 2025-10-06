import sys
from pathlib import Path

import pytest

from fractal_server.app.db import get_sync_db
from fractal_server.app.models import Profile
from fractal_server.app.models import Resource


@pytest.fixture(scope="function")
def local_resource_profile_objects(
    tmp777_path: Path,
    current_py_version: str,
) -> tuple[Resource, Profile]:
    """
    This fixture does not act on the db.
    """
    res = Resource(
        name="local resource 1",
        resource_type="local",
        job_local_folder=(tmp777_path / "jobs").as_posix(),
        tasks_local_folder=(tmp777_path / "tasks").as_posix(),
        job_runner_config={"parallel_tasks_per_job": 1},
        tasks_python_config={
            "default_version": current_py_version,
            "versions": {
                current_py_version: sys.executable,
            },
        },
    )
    prof = Profile(
        resource_id=123456789,
    )
    return res, prof


@pytest.fixture(scope="function")
def local_resource_profile_db(
    local_resource_profile_objects: tuple[Resource, Profile],
) -> tuple[Resource, Profile]:
    """
    This fixture does not act on the db.
    """
    res, prof = local_resource_profile_objects
    with next(get_sync_db()) as db_sync:
        # Create resource
        res.id = None
        db_sync.add(res)
        db_sync.commit()
        db_sync.refresh(res)

        # Create profile
        prof.id = None
        prof.resource_id = res.id
        db_sync.add(prof)
        db_sync.commit()
        db_sync.refresh(prof)

        db_sync.expunge_all()

    return res, prof
