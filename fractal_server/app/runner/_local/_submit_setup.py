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
Submodule to define _local_submit_setup
"""
from pathlib import Path
from typing import Optional

from ...models import WorkflowTask
from ..common import TaskParameters
from ._local_config import get_local_backend_config


def _local_submit_setup(
    *,
    wftask: WorkflowTask,
    workflow_dir: Optional[Path] = None,
    workflow_dir_user: Optional[Path] = None,
    task_pars: Optional[TaskParameters] = None,
) -> dict[str, object]:
    """
    Collect WorfklowTask-specific configuration parameters from different
    sources, and inject them for execution.

    Arguments:
        wftask:
            WorkflowTask for which the configuration is to be assembled
        task_pars:
            Not used in this function.
        workflow_dir:
            Not used in this function.
        workflow_dir_user:
            Not used in this function.

    Returns:
        submit_setup_dict:
            A dictionary that will be passed on to
            `FractalThreadPoolExecutor.submit` and
            `FractalThreadPoolExecutor.map`, so as to set extra options.
    """

    local_backend_config = get_local_backend_config(wftask=wftask)

    return dict(local_backend_config=local_backend_config)
