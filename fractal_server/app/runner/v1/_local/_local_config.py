# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Submodule to handle the local-backend configuration for a WorkflowTask
"""
import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from pydantic import Extra
from pydantic.error_wrappers import ValidationError

from .....config import get_settings
from .....syringe import Inject
from ....models.v1 import WorkflowTask


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
            `FractalThreadPoolExecutor.map`; if `None`, then all tasks will
            start at the same time.
    """

    parallel_tasks_per_job: Optional[int]


def get_default_local_backend_config():
    """
    Return a default `LocalBackendConfig` configuration object
    """
    return LocalBackendConfig(parallel_tasks_per_job=None)


def get_local_backend_config(
    wftask: WorkflowTask,
    config_path: Optional[Path] = None,
) -> LocalBackendConfig:
    """
    Prepare a `LocalBackendConfig` configuration object

    The sources for `parallel_tasks_per_job` attributes, starting from the
    highest-priority one, are

    1. Properties in `wftask.meta`;
    2. The general content of the local-backend configuration file;
    3. The default value (`None`).

    Arguments:
        wftask:
            WorkflowTask (V1) for which the backend configuration should
            be prepared.
        config_path:
            Path of local-backend configuration file; if `None`, use
            `FRACTAL_LOCAL_CONFIG_FILE` variable from settings.

    Returns:
        A local-backend configuration object
    """

    key = "parallel_tasks_per_job"
    default = None

    if wftask.meta and key in wftask.meta:
        parallel_tasks_per_job = wftask.meta[key]
    else:
        if not config_path:
            settings = Inject(get_settings)
            config_path = settings.FRACTAL_LOCAL_CONFIG_FILE
        if config_path is None:
            parallel_tasks_per_job = default
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

            parallel_tasks_per_job = env.get(key, default)
    return LocalBackendConfig(parallel_tasks_per_job=parallel_tasks_per_job)
