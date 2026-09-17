import time
from pathlib import Path

from utils_for_orm_dump import REPETITIONS
from utils_for_orm_dump import expected_task_group_dict
from utils_for_orm_dump import expected_user_dict
from utils_for_orm_dump import report
from utils_for_orm_dump import task_group_orm_object
from utils_for_orm_dump import user_orm_object

from fractal_server.app.models import orm_model_to_dict


def profile(fn, *args, **kwargs):
    timings = []
    for _ in range(REPETITIONS):
        start = time.perf_counter()
        fn(*args, **kwargs)
        stop = time.perf_counter()
        timings.append(stop - start)
    return timings


### CHECKS

# Assert full output
assert orm_model_to_dict(user_orm_object) == expected_user_dict
assert orm_model_to_dict(task_group_orm_object) == expected_task_group_dict

# Check that we only moved from an ORM model to a dictionary, without making it
# a JSON-serializable one
assert isinstance(orm_model_to_dict(task_group_orm_object)["path"], Path)

# Check exclude
assert "profile_id" not in orm_model_to_dict(
    obj=user_orm_object, exclude={"profile_id"}
)

# Check include
assert "venv_path" not in orm_model_to_dict(
    obj=task_group_orm_object, include={"pkg_name", "version"}
)

### BENCHMARKS

timings = profile(
    orm_model_to_dict, obj=user_orm_object, exclude={"profile_id"}
)
report("UserOAuth", timings)
timings = profile(
    orm_model_to_dict, obj=task_group_orm_object, exclude={"id", "venv_path"}
)
report("TaskGroupV2", timings)
