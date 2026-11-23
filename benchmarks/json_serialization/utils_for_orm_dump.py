import statistics
from datetime import datetime
from datetime import timezone
from typing import Any

from sqlalchemy.orm import Session

from fractal_server.app.models import Profile
from fractal_server.app.models import Resource
from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth

REPETITIONS = 20


def report(name: str, timings: list[float]) -> float:
    mean = statistics.mean(timings)
    print(
        f"[{name:>11s}]: mean={mean * 1e6:4.2f} ns  "
        f"median={statistics.median(timings) * 1e6:4.2f} ns  "
        f"min={min(timings) * 1e6:4.2f} ns  "
        f"max={max(timings) * 1e6:4.2f} ns"
    )
    return mean


_NOW = datetime.now(tz=timezone.utc)


def get_orm_objects(
    db: Session,
) -> tuple[Resource, Profile, UserOAuth, TaskGroupV2]:
    resource_orm_object = Resource(
        type="local",
        name="my resource",
        timestamp_created=_NOW,
        jobs_local_dir="/something",
        tasks_local_dir="/something",
        jobs_runner_config={
            "very": [
                "complex",
                {"nested": "object", "path": "/path"},
            ]
        },
        jobs_poll_interval=1,
    )
    db.add(resource_orm_object)
    db.flush()

    profile_orm_object = Profile(
        resource_id=resource_orm_object.id,
        name="My profile",
        resource_type="local",
    )
    db.add(profile_orm_object)
    db.flush()

    user_orm_object = UserOAuth(
        email="email@example.org",
        hashed_password="xxxx",
        is_active=True,
        is_superuser=False,
        is_verified=True,
        is_guest=False,
        profile_id=profile_orm_object.id,
        project_dirs=["/something"],
        slurm_accounts=["1", "2", "3"],
    )
    db.add(user_orm_object)
    db.flush()

    task_group_orm_object = TaskGroupV2(
        id=1,
        task_list=[],
        user_id=user_orm_object.id,
        user_group_id=None,
        resource_id=resource_orm_object.id,
        origin="wheel",
        pkg_name="pkg_name",
        version="1.2.3",
        python_version="3.14",
        pixi_version=None,
        path="/some/path",
        archive_path="/some/other/path",
        pinned_package_versions_pre={
            f"package-{ind}": f"version-{ind}" for ind in range(50)
        },
        pinned_package_versions_post={
            f"package-{ind}": f"version-{ind}" for ind in range(50)
        },
        env_info=None,
        venv_path=None,
        active=True,
        timestamp_created=_NOW,
        timestamp_last_used=_NOW,
    )

    db.add(task_group_orm_object)
    db.commit()

    for obj_orm in (
        resource_orm_object,
        profile_orm_object,
        user_orm_object,
        task_group_orm_object,
    ):
        db.refresh(obj_orm)

    return (
        resource_orm_object,
        profile_orm_object,
        user_orm_object,
        task_group_orm_object,
    )


def get_expected_dicts() -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    resource = {
        "prevent_new_submissions": False,
        "type": "local",
        "jobs_poll_interval": 1,
        "jobs_local_dir": "/something",
        "timestamp_created": _NOW,
        "jobs_runner_config": {
            "very": ["complex", {"path": "/path", "nested": "object"}]
        },
        "tasks_local_dir": "/something",
        "tasks_pixi_config": {},
        "tasks_python_config": {},
        "id": 1,
        "jobs_slurm_python_worker": None,
        "host": None,
        "name": "my resource",
    }
    profile = {
        "pixi_cache_dir": None,
        "username": None,
        "ssh_key_path": None,
        "resource_type": "local",
        "tasks_remote_dir": None,
        "name": "My profile",
        "id": 1,
        "jobs_remote_dir": None,
        "resource_id": 1,
    }
    user = {
        "slurm_accounts": ["1", "2", "3"],
        "hashed_password": "xxxx",
        "email": "email@example.org",
        "profile_id": 1,
        "is_superuser": False,
        "is_guest": False,
        "is_active": True,
        "id": 1,
        "is_verified": True,
        "project_dirs": ["/something"],
    }
    taskgroup = {
        "user_group_id": None,
        "pkg_name": "pkg_name",
        "resource_id": 1,
        "origin": "wheel",
        "user_id": 1,
        "archive_path": "/some/other/path",
        "id": 1,
        "timestamp_last_used": _NOW,
        "version": "1.2.3",
        "env_info": None,
        "active": True,
        "python_version": "3.14",
        "venv_path": None,
        "timestamp_created": _NOW,
        "pip_extras": None,
        "pinned_package_versions_pre": {
            "package-0": "version-0",
            "package-1": "version-1",
            "package-2": "version-2",
            "package-3": "version-3",
            "package-4": "version-4",
            "package-5": "version-5",
            "package-6": "version-6",
            "package-7": "version-7",
            "package-8": "version-8",
            "package-9": "version-9",
            "package-10": "version-10",
            "package-11": "version-11",
            "package-12": "version-12",
            "package-13": "version-13",
            "package-14": "version-14",
            "package-15": "version-15",
            "package-16": "version-16",
            "package-17": "version-17",
            "package-18": "version-18",
            "package-19": "version-19",
            "package-20": "version-20",
            "package-21": "version-21",
            "package-22": "version-22",
            "package-23": "version-23",
            "package-24": "version-24",
            "package-25": "version-25",
            "package-26": "version-26",
            "package-27": "version-27",
            "package-28": "version-28",
            "package-29": "version-29",
            "package-30": "version-30",
            "package-31": "version-31",
            "package-32": "version-32",
            "package-33": "version-33",
            "package-34": "version-34",
            "package-35": "version-35",
            "package-36": "version-36",
            "package-37": "version-37",
            "package-38": "version-38",
            "package-39": "version-39",
            "package-40": "version-40",
            "package-41": "version-41",
            "package-42": "version-42",
            "package-43": "version-43",
            "package-44": "version-44",
            "package-45": "version-45",
            "package-46": "version-46",
            "package-47": "version-47",
            "package-48": "version-48",
            "package-49": "version-49",
        },
        "pixi_version": None,
        "path": "/some/path",
        "pinned_package_versions_post": {
            "package-0": "version-0",
            "package-1": "version-1",
            "package-2": "version-2",
            "package-3": "version-3",
            "package-4": "version-4",
            "package-5": "version-5",
            "package-6": "version-6",
            "package-7": "version-7",
            "package-8": "version-8",
            "package-9": "version-9",
            "package-10": "version-10",
            "package-11": "version-11",
            "package-12": "version-12",
            "package-13": "version-13",
            "package-14": "version-14",
            "package-15": "version-15",
            "package-16": "version-16",
            "package-17": "version-17",
            "package-18": "version-18",
            "package-19": "version-19",
            "package-20": "version-20",
            "package-21": "version-21",
            "package-22": "version-22",
            "package-23": "version-23",
            "package-24": "version-24",
            "package-25": "version-25",
            "package-26": "version-26",
            "package-27": "version-27",
            "package-28": "version-28",
            "package-29": "version-29",
            "package-30": "version-30",
            "package-31": "version-31",
            "package-32": "version-32",
            "package-33": "version-33",
            "package-34": "version-34",
            "package-35": "version-35",
            "package-36": "version-36",
            "package-37": "version-37",
            "package-38": "version-38",
            "package-39": "version-39",
            "package-40": "version-40",
            "package-41": "version-41",
            "package-42": "version-42",
            "package-43": "version-43",
            "package-44": "version-44",
            "package-45": "version-45",
            "package-46": "version-46",
            "package-47": "version-47",
            "package-48": "version-48",
            "package-49": "version-49",
        },
    }

    return (resource, profile, user, taskgroup)
