import statistics
from datetime import datetime
from datetime import timezone
from pathlib import Path

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


user_orm_object = UserOAuth(
    id=1,
    email="email@example.org",
    hashed_password="xxxx",
    is_active=True,
    is_superuser=False,
    is_verified=True,
    is_guest=False,
    profile_id=1,
    project_dirs=["/something"],
    slurm_accounts=["1", "2", "3"],
)

expected_user_dict = {
    "is_active": True,
    "project_dirs": ["/something"],
    "hashed_password": "xxxx",
    "is_guest": False,
    "email": "email@example.org",
    "slurm_accounts": ["1", "2", "3"],
    "id": 1,
    "is_verified": True,
    "profile_id": 1,
    "is_superuser": False,
}

now = datetime.now(tz=timezone.utc)

task_group_orm_object = TaskGroupV2(
    id=1,
    task_list=[],
    user_id=1,
    user_group_id=2,
    resource_id=3,
    origin="wheel",
    pkg_name="pkg_name",
    version="1.2.3",
    python_version="3.14",
    pixi_version=None,
    path=Path("/some/path"),
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
    timestamp_created=now,
    timestamp_last_used=now,
)

expected_task_group_dict = {
    "timestamp_last_used": now,
    "user_group_id": 2,
    "resource_id": 3,
    "version": "1.2.3",
    "archive_path": "/some/other/path",
    "pkg_name": "pkg_name",
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
    "venv_path": None,
    "pixi_version": None,
    "active": True,
    "timestamp_created": now,
    "origin": "wheel",
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
    "path": Path("/some/path"),
    "python_version": "3.14",
    "id": 1,
    "user_id": 1,
    "env_info": None,
}
