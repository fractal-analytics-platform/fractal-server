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
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import ValidationError

from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


class LocalBackendConfigError(ValueError):
    """
    Local-backend configuration error
    """

    pass


class LocalBackendConfig(BaseModel):
    """
    Specifications of the local-backend configuration

    Attributes:
        parallel_tasks_per_job:
            Maximum number of tasks to be run in parallel as part of a call to
            `FractalThreadPoolExecutor.map`; if `None`, then all tasks will
            start at the same time.
    """

    model_config = ConfigDict(extra="forbid")
    parallel_tasks_per_job: int | None = None

    @property
    def batch_size(self) -> int:
        return self.parallel_tasks_per_job or 1


def get_local_backend_config(
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
    config_path: Path | None = None,
    tot_tasks: int = 1,
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

    key = "parallel_tasks_per_job"
    default_value = None

    if which_type == "non_parallel":
        wftask_meta = wftask.meta_non_parallel
    elif which_type == "parallel":
        wftask_meta = wftask.meta_parallel
    else:
        raise ValueError(
            "`get_local_backend_config` received an invalid argument"
            f" {which_type=}."
        )

    if wftask_meta and key in wftask_meta:
        parallel_tasks_per_job = wftask_meta[key]
    else:
        if not config_path:
            settings = Inject(get_settings)
            config_path = settings.FRACTAL_LOCAL_CONFIG_FILE
        if config_path is None:
            parallel_tasks_per_job = default_value
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

            parallel_tasks_per_job = env.get(key, default_value)
    return LocalBackendConfig(parallel_tasks_per_job=parallel_tasks_per_job)
