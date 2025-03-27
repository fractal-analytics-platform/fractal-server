# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Submodule to define _slurm_submit_setup, which is also the reference
implementation of `submit_setup_call`.
"""
from typing import Any
from typing import Literal

from fractal_server.app.models.v2 import WorkflowTaskV2
from fractal_server.app.runner.executors.slurm_common.get_slurm_config import (
    get_slurm_config,
)


def _slurm_submit_setup(
    *,
    wftask: WorkflowTaskV2,
    which_type: Literal["non_parallel", "parallel"],
) -> dict[str, Any]:
    """
    Collect WorkflowTask-specific configuration parameters from different
    sources, and inject them for execution.


    Arguments:
        wftask:
        which_type:
    """

    # Get SlurmConfig object
    slurm_config = get_slurm_config(wftask=wftask, which_type=which_type)

    return dict(slurm_config=slurm_config)
