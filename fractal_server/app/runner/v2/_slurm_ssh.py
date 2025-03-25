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

This backend runs fractal workflows in a SLURM cluster using Clusterfutures
Executor objects.
"""
from pathlib import Path
from typing import Optional

from ....ssh._fabric import FractalSSH
from ...models.v2 import DatasetV2
from ...models.v2 import WorkflowV2
from ..exceptions import JobExecutionError
from ..executors.slurm_common._submit_setup import _slurm_submit_setup
from ..executors.slurm_ssh.executor import FractalSlurmSSHExecutor
from ..set_start_and_last_task_index import set_start_and_last_task_index
from .runner import execute_tasks_v2
from fractal_server.images.models import AttributeFiltersType
from fractal_server.logger import set_logger

logger = set_logger(__name__)


def process_workflow(
    *,
    workflow: WorkflowV2,
    dataset: DatasetV2,
    workflow_dir_local: Path,
    workflow_dir_remote: Optional[Path] = None,
    first_task_index: Optional[int] = None,
    last_task_index: Optional[int] = None,
    logger_name: str,
    job_attribute_filters: AttributeFiltersType,
    fractal_ssh: FractalSSH,
    worker_init: Optional[str] = None,
    user_id: int,
    **kwargs,  # not used
) -> None:
    """
    Process workflow (SLURM backend public interface)
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

    # Create main remote folder
    try:
        fractal_ssh.mkdir(folder=str(workflow_dir_remote))
        logger.info(f"Created {str(workflow_dir_remote)} via SSH.")
    except Exception as e:
        error_msg = (
            f"Could not create {str(workflow_dir_remote)} via SSH.\n"
            f"Original error: {str(e)}."
        )
        logger.error(error_msg)
        raise JobExecutionError(info=error_msg)

    with FractalSlurmSSHExecutor(
        fractal_ssh=fractal_ssh,
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        common_script_lines=worker_init,
    ) as executor:
        execute_tasks_v2(
            wf_task_list=workflow.task_list[
                first_task_index : (last_task_index + 1)
            ],
            dataset=dataset,
            runner=executor,
            workflow_dir_local=workflow_dir_local,
            workflow_dir_remote=workflow_dir_remote,
            logger_name=logger_name,
            submit_setup_call=_slurm_submit_setup,
            job_attribute_filters=job_attribute_filters,
            user_id=user_id,
        )
