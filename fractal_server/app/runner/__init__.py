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
Runner backend subsystem root

This module is the single entry point to the runner backend subsystem. Other
subystems should only import this module and not its submodules or the
individual backends.
"""
import logging
import os
from pathlib import Path
from typing import Dict
from typing import Optional

from ... import __VERSION__
from ...config import get_settings
from ...syringe import Inject
from ...utils import set_logger
from ..db import DB
from ..models import ApplyWorkflow
from ..models import Dataset
from ..models import JobStatusType
from ..models import Workflow
from ._local import process_workflow as local_process_workflow
from .common import auto_output_dataset  # noqa: F401
from .common import close_job_logger
from .common import JobExecutionError
from .common import TaskExecutionError
from .common import validate_workflow_compatibility  # noqa: F401


_backends = {}
_backend_errors: Dict[str, Exception] = {}

_backends["local"] = local_process_workflow

try:
    from ._slurm import process_workflow as slurm_process_workflow

    _backends["slurm"] = slurm_process_workflow
except ModuleNotFoundError as e:
    _backend_errors["slurm"] = e


def get_process_workflow():
    settings = Inject(get_settings)
    try:
        process_workflow = _backends[settings.FRACTAL_RUNNER_BACKEND]
    except KeyError:
        raise _backend_errors.get(
            settings.FRACTAL_RUNNER_BACKEND,
            RuntimeError(
                "Unknown error during collection of backend "
                f"`{settings.FRACTAL_RUNNER_BACKEND}`"
            ),
        )
    return process_workflow


async def submit_workflow(
    *,
    workflow: Workflow,
    input_dataset: Dataset,
    output_dataset: Dataset,
    job_id: int,
    project_dir: str,
    slurm_user: Optional[str] = None,
    worker_init: Optional[str] = None,
) -> None:
    """
    Prepares a workflow and applies it to a dataset

    This function wraps the process_workflow one, which is different for each
    backend (e.g. local or slurm backend).

    Args:
        workflow:
            Workflow being applied
        input_dataset:
            Input dataset
        output_dataset:
            the destination dataset of the workflow. If not provided,
            overwriting of the input dataset is implied and an error is raised
            if the dataset is in read only mode. If a string is passed and the
            dataset does not exist, a new dataset with that name is created and
            within it a new resource with the same name.
        job_id:
            Id of the job record which stores the state for the current
            workflow application.
        project_dir:
            Project directory (namely a path where the user can write). For the
            slurm backend, this is used as a base directory for
            `job.working_dir_user`.
        slurm_user:
            The username to impersonate for the workflow execution.
        worker_init:
            Custom executor parameters that get parsed before the execution of
            each task.
    """
    db_sync = next(DB.get_sync_db())
    job: ApplyWorkflow = db_sync.get(ApplyWorkflow, job_id)  # type: ignore
    if not job:
        raise ValueError("Cannot fetch job from database")

    # Select backend
    settings = Inject(get_settings)
    FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
    process_workflow = get_process_workflow()

    # Prepare some of process_workflow arguments
    input_paths = input_dataset.paths
    output_path = output_dataset.paths[0]
    workflow_id = workflow.id

    # Define and create server-side working folder
    WORKFLOW_DIR = (
        settings.FRACTAL_RUNNER_WORKING_BASE_DIR  # type: ignore
        / f"workflow_{workflow_id:06d}_job_{job_id:06d}"
    ).resolve()
    if not WORKFLOW_DIR.exists():
        original_umask = os.umask(0)
        WORKFLOW_DIR.mkdir(parents=True, mode=0o755)
        os.umask(original_umask)

    # Define and create user-side working folder, if needed
    if FRACTAL_RUNNER_BACKEND == "local":
        WORKFLOW_DIR_USER = WORKFLOW_DIR
    elif FRACTAL_RUNNER_BACKEND == "slurm":
        from ._slurm._subprocess_run_as_user import _mkdir_as_user

        WORKFLOW_DIR_USER = (Path(project_dir) / WORKFLOW_DIR.name).resolve()
        _mkdir_as_user(folder=str(WORKFLOW_DIR_USER), user=slurm_user)
    else:
        raise ValueError(f"{FRACTAL_RUNNER_BACKEND=} not supported")

    # Update db
    job.working_dir = WORKFLOW_DIR.as_posix()
    job.working_dir_user = WORKFLOW_DIR_USER.as_posix()
    job.status = JobStatusType.RUNNING
    db_sync.merge(job)
    db_sync.commit()

    # Write logs
    logger_name = f"WF{workflow_id}_job{job_id}"
    logger = set_logger(
        logger_name=logger_name,
        log_file_path=WORKFLOW_DIR / "workflow.log",
        level=logging.INFO,
        formatter=logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"),
    )
    logger.info(f"fractal_server.__VERSION__: {__VERSION__}")
    logger.info(f"FRACTAL_RUNNER_BACKEND: {FRACTAL_RUNNER_BACKEND}")
    logger.info(f"slurm_user: {slurm_user}")
    logger.info(f"worker_init: {worker_init}")
    logger.info(f"input metadata: {input_dataset.meta}")
    logger.info(f"input_paths: {input_paths}")
    logger.info(f"output_path: {output_path}")
    logger.info(f"job.id: {job.id}")
    logger.info(f"job.working_dir: {str(WORKFLOW_DIR)}")
    logger.info(f"job.workflow_dir_user: {str(WORKFLOW_DIR_USER)}")
    logger.info(f'START workflow "{workflow.name}"')

    try:
        output_dataset.meta = await process_workflow(
            workflow=workflow,
            input_paths=input_paths,
            output_path=output_path,
            input_metadata=input_dataset.meta,
            slurm_user=slurm_user,
            workflow_dir=WORKFLOW_DIR,
            workflow_dir_user=WORKFLOW_DIR_USER,
            logger_name=logger_name,
            worker_init=worker_init,
        )

        logger.info(f'END workflow "{workflow.name}"')
        close_job_logger(logger)

        db_sync.merge(output_dataset)
        job.status = JobStatusType.DONE
        db_sync.merge(job)

    except TaskExecutionError as e:

        logger.info(f'FAILED workflow "{workflow.name}", TaskExecutionError.')
        close_job_logger(logger)

        job.status = JobStatusType.FAILED

        exception_args_string = "\n".join(e.args)
        job.log = (
            f"TASK ERROR:"
            f"Task id: {e.workflow_task_id} ({e.task_name}), "
            f"{e.workflow_task_order=}\n"
            f"TRACEBACK:\n{exception_args_string}"
        )
        db_sync.merge(job)

    except JobExecutionError as e:

        logger.info(f'FAILED workflow "{workflow.name}", JobExecutionError.')
        close_job_logger(logger)

        job.status = JobStatusType.FAILED
        error = e.assemble_error()
        job.log = f"JOB ERROR:\nTRACEBACK:\n{error}"
        db_sync.merge(job)

    except Exception as e:

        logger.info(f'FAILED workflow "{workflow.name}", unknown error.')
        close_job_logger(logger)

        job.status = JobStatusType.FAILED
        job.log = f"UNKNOWN ERROR\nOriginal error: {str(e)}"
        db_sync.merge(job)

    finally:
        db_sync.commit()
