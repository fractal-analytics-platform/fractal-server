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

from ...models.v2 import DatasetV2
from ...models.v2 import WorkflowV2
from ..executors.slurm_common.get_slurm_config import get_slurm_config
from ..executors.slurm_sudo.runner import SudoSlurmRunner
from ..set_start_and_last_task_index import set_start_and_last_task_index
from .runner import execute_tasks_v2
from fractal_server.types import AttributeFilters


def process_workflow(
    *,
    workflow: WorkflowV2,
    dataset: DatasetV2,
    workflow_dir_local: Path,
    job_id: int,
    workflow_dir_remote: Path | None = None,
    first_task_index: int | None = None,
    last_task_index: int | None = None,
    logger_name: str,
    job_attribute_filters: AttributeFilters,
    job_type_filters: dict[str, bool],
    user_id: int,
    # SLURM-sudo-specific
    user_cache_dir: str | None = None,
    slurm_user: str | None = None,
    slurm_account: str | None = None,
    worker_init: str | None = None,
) -> None:
    """
    Process workflow (SLURM backend public interface).
    """

    # Set values of first_task_index and last_task_index
    num_tasks = len(workflow.task_list)
    first_task_index, last_task_index = set_start_and_last_task_index(
        num_tasks,
        first_task_index=first_task_index,
        last_task_index=last_task_index,
    )

    if not slurm_user:
        raise RuntimeError(
            "slurm_user argument is required, for slurm backend"
        )

    if isinstance(worker_init, str):
        worker_init = worker_init.split("\n")

    with SudoSlurmRunner(
        slurm_user=slurm_user,
        user_cache_dir=user_cache_dir,
        root_dir_local=workflow_dir_local,
        root_dir_remote=workflow_dir_remote,
        common_script_lines=worker_init,
        slurm_account=slurm_account,
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
