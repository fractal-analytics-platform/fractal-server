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
from pathlib import Path
from typing import Literal

from ...task_files import get_task_file_paths
from .get_slurm_config import get_slurm_config
from fractal_server.app.models.v2 import WorkflowTaskV2


def _slurm_submit_setup(
    *,
    wftask: WorkflowTaskV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Path,
    which_type: Literal["non_parallel", "parallel"],
) -> dict[str, object]:
    """
    Collect WorfklowTask-specific configuration parameters from different
    sources, and inject them for execution.

    Here goes all the logic for reading attributes from the appropriate sources
    and transforming them into an appropriate `SlurmConfig` object (encoding
    SLURM configuration) and `TaskFiles` object (with details e.g. about file
    paths or filename prefixes).

    For now, this is the reference implementation for the argument
    `submit_setup_call` of
    [fractal_server.app.runner.v2.runner][].

    Arguments:
        wftask:
            WorkflowTask for which the configuration is to be assembled
        workflow_dir_local:
            Server-owned directory to store all task-execution-related relevant
            files (inputs, outputs, errors, and all meta files related to the
            job execution). Note: users cannot write directly to this folder.
        workflow_dir_remote:
            User-side directory with the same scope as `workflow_dir_local`,
            and where a user can write.

    Returns:
        submit_setup_dict:
            A dictionary that will be passed on to
            `FractalSlurmExecutor.submit` and `FractalSlurmExecutor.map`, so
            as to set extra options.
    """

    # Get SlurmConfig object
    slurm_config = get_slurm_config(
        wftask=wftask,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        which_type=which_type,
    )

    # Get TaskFiles object
    task_files = get_task_file_paths(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_order=wftask.order,
        task_name=wftask.task.name,
    )

    # Prepare and return output dictionary
    submit_setup_dict = dict(
        slurm_config=slurm_config,
        task_files=task_files,
    )
    return submit_setup_dict
