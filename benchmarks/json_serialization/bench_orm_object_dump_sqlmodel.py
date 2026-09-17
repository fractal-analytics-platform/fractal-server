import time
from pathlib import Path

from pydantic import BaseModel
from utils_for_orm_dump import REPETITIONS
from utils_for_orm_dump import expected_user_dict
from utils_for_orm_dump import report
from utils_for_orm_dump import task_group_orm_object
from utils_for_orm_dump import user_orm_object


def profile(obj: BaseModel, **kwargs):
    timings = []
    for _ in range(REPETITIONS):
        start = time.perf_counter()
        obj.model_dump(**kwargs)
        stop = time.perf_counter()
        timings.append(stop - start)
    return timings


### CHECKS

# Assert full output
assert user_orm_object.model_dump() == expected_user_dict

# Check that we only moved from an ORM model to a dictionary, without making it
# a JSON-serializable one
assert isinstance(task_group_orm_object.model_dump()["path"], Path)

# Check exclude
assert "profile_id" not in user_orm_object.model_dump(exclude={"profile_id"})

# Check include
assert "venv_path" not in task_group_orm_object.model_dump(
    include={"pkg_name", "version"}
)

### BENCHMARKS

timings = profile(user_orm_object, exclude={"profile_id"})
report("UserOAuth", timings)
timings = profile(task_group_orm_object, exclude={"id", "venv_path"})
report("TaskGroupV2", timings)
