# Copyright 2022 (C) Friedrich Miescher Institute for Biomedical Research and
# University of Zurich
#
# Original authors:
# Jacopo Nespolo <jacopo.nespolo@exact-lab.it>
# Tommaso Comparin <tommaso.comparin@exact-lab.it>
# Marco Franzon <marco.franzon@exact-lab.it>
#
# This file is part of Fractal and was originally developed by eXact lab S.r.l.
# <exact-lab.it> under contract with Liberali Lab from the Friedrich Miescher
# Institute for Biomedical Research and Pelkmans Lab from the University of
# Zurich.
"""
Slurm Backend

This backend runs fractal workflows in a SLURM cluster.
"""
from pathlib import Path

from ...ssh._fabric import FractalSSH
from ..executors.slurm_common.get_slurm_config import get_slurm_config
from ..executors.slurm_ssh.runner import SlurmSSHRunner
from ..set_start_and_last_task_index import set_start_and_last_task_index
from .runner import execute_tasks_v2
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.logger import set_logger
from fractal_server.types import AttributeFilters

logger = set_logger(__name__)


def process_workflow(
    *,
    job_id: int,
    workflow: WorkflowV2,
    dataset: DatasetV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Path | None = None,
    first_task_index: int | None = None,
    last_task_index: int | None = None,
    logger_name: str,
    job_attribute_filters: AttributeFilters,
    job_type_filters: dict[str, bool],
    user_id: int,
    resource: Resource,
    profile: Profile,
    fractal_ssh: FractalSSH | None = None,
    slurm_account: str | None = None,
    worker_init: str | None = None,
    user_cache_dir: str,
) -> None:
    """
    Run a workflow through a `slurm_ssh` backend.

        Args:
        job_id: Job ID.
        workflow: Workflow to be run
        dataset: Dataset to be used.
        workflow_dir_local: Local working directory for this job.
        workflow_dir_remote:
            Remote working directory for this job - only relevant for
            `slurm_sudo` and `slurm_ssh` backends.
        first_task_index:
            Positional index of the first task to execute; if `None`, start
            from `0`.
        last_task_index:
            Positional index of the last task to execute; if `None`, proceed
            until the last task.
        logger_name: Logger name
        user_id: User ID.
        resource: Computational resource for running this job.
        profile: Computational profile for running this job.
        user_cache_dir:
            User-writeable folder (typically a subfolder of `project_dir`).
            Only relevant for `slurm_sudo` and `slurm_ssh` backends.
        fractal_ssh:
            `FractalSSH` object, only relevant for the `slurm_ssh` backend.
        slurm_account:
            SLURM account to set.
            Only relevant for `slurm_sudo` and `slurm_ssh` backends.
        worker_init:
            Additional preamble lines for SLURM submission script.
            Only relevant for `slurm_sudo` and `slurm_ssh` backends.
    """

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    if isinstance(worker_init, str):
        worker_init = worker_init.split("\n")

    with SlurmSSHRunner(
        fractal_ssh=fractal_ssh,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        slurm_account=slurm_account,
        resource=resource,
        profile=profile,
        common_script_lines=worker_init,
        user_cache_dir=user_cache_dir,
    ) as runner:
        execute_tasks_v2(
            wf_task_list=workflow.task_list[
                first_task_index : (last_task_index + 1)
            ],
            dataset=dataset,
            job_id=job_id,
            runner=runner,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
            logger_name=logger_name,
            get_runner_config=get_slurm_config,
            job_attribute_filters=job_attribute_filters,
            job_type_filters=job_type_filters,
            user_id=user_id,
        )
