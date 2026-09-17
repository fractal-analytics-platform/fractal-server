import statistics
import time
from datetime import datetime
from datetime import timezone

from pydantic import BaseModel

from fractal_server.app.models import TaskGroupV2
from fractal_server.app.models import UserOAuth

REPETITIONS = 20


def profile(obj: BaseModel, **kwargs):
    timings = []
    for _ in range(REPETITIONS):
        start = time.perf_counter()
        obj.model_dump(**kwargs)
        stop = time.perf_counter()
        timings.append(stop - start)
    return timings


def report(name: str, timings: list[float]) -> float:
    mean = statistics.mean(timings)
    print(
        f"[{name:>11s}]: mean={mean * 1e6:4.2f} ns  "
        f"median={statistics.median(timings) * 1e6:4.2f} ns  "
        f"min={min(timings) * 1e6:4.2f} ns  "
        f"max={max(timings) * 1e6:4.2f} ns"
    )
    return mean


u = UserOAuth(
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

tg = TaskGroupV2(
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
    timestamp_created=datetime.now(tz=timezone.utc),
    timestamp_last_used=datetime.now(tz=timezone.utc),
)


assert "profile_id" not in u.model_dump(exclude={"profile_id"})
assert "venv_path" not in tg.model_dump(include={"pkg_name", "version"})

timings = profile(obj=u, exclude={"profile_id"})
report("UserOAuth", timings)
timings = profile(obj=tg, exclude={"id", "venv_path"})
report("TaskGroupV2", timings)
