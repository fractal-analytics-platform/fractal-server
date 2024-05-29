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

from ....models.v1 import WorkflowTask
from ._local_config import get_local_backend_config


def _local_submit_setup(
    *,
    wftask: WorkflowTask,
    workflow_dir_local: Optional[Path] = None,
    workflow_dir_remote: Optional[Path] = None,
) -> dict[str, object]:
    """
    Collect WorfklowTask-specific configuration parameters from different
    sources, and inject them for execution.

    Arguments:
        wftask:
            WorkflowTask for which the configuration is to be assembled
        workflow_dir_local:
            Not used in this function.
        workflow_dir_remote:
            Not used in this function.

    Returns:
        submit_setup_dict:
            A dictionary that will be passed on to
            `FractalThreadPoolExecutor.submit` and
            `FractalThreadPoolExecutor.map`, so as to set extra options.
    """

    local_backend_config = get_local_backend_config(wftask=wftask)

    return dict(local_backend_config=local_backend_config)
