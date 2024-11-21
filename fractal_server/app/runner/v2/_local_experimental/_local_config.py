"""
Submodule to handle the local-backend configuration for a WorkflowTask
"""
import json
from pathlib import Path
from typing import Literal
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic.error_wrappers import ValidationError

from .....config import get_settings
from .....syringe import Inject
from ....models.v2 import WorkflowTaskV2


class LocalBackendConfigError(ValueError):
    """
    Local-backend configuration error
    """

    pass


class LocalBackendConfig(BaseModel, extra=Extra.forbid):
    """
    Specifications of the local-backend configuration

    Attributes:
        parallel_tasks_per_job:
            Maximum number of tasks to be run in parallel as part of a call to
            `FractalProcessPoolExecutor.map`; if `None`, then all tasks will
            start at the same time.
        max_workers;
            From `ProcessPoolExecutor` docs: "If max_workers is None or not
            given, it will default to `os.process_cpu_count()`."
    """

    parallel_tasks_per_job: Optional[int]
    max_workers: Optional[int] = None


def get_default_local_backend_config():
    """
    Return a default `LocalBackendConfig` configuration object
    """
    return LocalBackendConfig(
        parallel_tasks_per_job=None,
        max_workers=None,
    )


def get_local_backend_config(
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
    config_path: Optional[Path] = None,
) -> LocalBackendConfig:
    """
    Prepare a `LocalBackendConfig` configuration object

    The sources for `parallel_tasks_per_job` attributes, starting from the
    highest-priority one, are

    1. Properties in `wftask.meta_parallel` or `wftask.meta_non_parallel`
       (depending on `which_type`);
    2. The general content of the local-backend configuration file;
    3. The default value (`None`).

    Arguments:
        wftask:
            WorkflowTaskV2 for which the backend configuration should
            be prepared.
        config_path:
            Path of local-backend configuration file; if `None`, use
            `FRACTAL_LOCAL_CONFIG_FILE` variable from settings.

    Returns:
        A local-backend configuration object
    """

    if which_type == "non_parallel":
        wftask_meta = wftask.meta_non_parallel
    elif which_type == "parallel":
        wftask_meta = wftask.meta_parallel
    else:
        raise ValueError(
            "`get_local_backend_config` received an invalid argument"
            f" {which_type=}."
        )

    config_dict = {}
    for key in ["parallel_tasks_per_job", "max_workers"]:
        if wftask_meta and key in wftask_meta:
            config_dict[key] = wftask_meta[key]
        else:
            if not config_path:
                settings = Inject(get_settings)
                config_path = settings.FRACTAL_LOCAL_CONFIG_FILE
            if config_path is None:
                config_dict[key] = None
            else:
                with config_path.open("r") as f:
                    env = json.load(f)
                try:
                    _ = LocalBackendConfig(**env)
                except ValidationError as e:
                    raise LocalBackendConfigError(
                        f"Error while loading {config_path=}. "
                        f"Original error:\n{str(e)}"
                    )
                config_dict[key] = env.get(key, None)
    return LocalBackendConfig(**config_dict)
