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
import os
import traceback
from pathlib import Path
from typing import Optional

from ... import __VERSION__
from ...config import get_settings
from ...logger import set_logger
from ...syringe import Inject
from ...utils import get_timestamp
from ..db import DB
from ..models import ApplyWorkflow
from ..models import Dataset
from ..models import JobStatusType
from ..models import Workflow
from ..models import WorkflowTask
from ._local import process_workflow as local_process_workflow
from .common import close_job_logger
from .common import JobExecutionError
from .common import TaskExecutionError
from .common import validate_workflow_compatibility  # noqa: F401
from .handle_failed_job import assemble_history_failed_job
from .handle_failed_job import assemble_meta_failed_job


_backends = {}
_backend_errors: dict[str, Exception] = {}
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
    workflow_id: int,
    input_dataset_id: int,
    output_dataset_id: int,
    job_id: int,
    worker_init: Optional[str] = None,
    slurm_user: Optional[str] = None,
    user_cache_dir: Optional[str] = None,
) -> None:
    """
    Prepares a workflow and applies it to a dataset

    This function wraps the process_workflow one, which is different for each
    backend (e.g. local or slurm backend).

    Args:
        workflow_id:
            ID of the workflow being applied
        input_dataset_id:
            Input dataset ID
        output_dataset_id:
            ID of the destination dataset of the workflow.
        job_id:
            Id of the job record which stores the state for the current
            workflow application.
        worker_init:
            Custom executor parameters that get parsed before the execution of
            each task.
        user_cache_dir:
            Cache directory (namely a path where the user can write); for the
            slurm backend, this is used as a base directory for
            `job.working_dir_user`.
        slurm_user:
            The username to impersonate for the workflow execution, for the
            slurm backend.
    """
    with next(DB.get_sync_db()) as db_sync:

        job: ApplyWorkflow = db_sync.get(ApplyWorkflow, job_id)
        if not job:
            raise ValueError(f"Cannot fetch job {job_id} from database")

        input_dataset: Dataset = db_sync.get(Dataset, input_dataset_id)
        output_dataset: Dataset = db_sync.get(Dataset, output_dataset_id)
        workflow: Workflow = db_sync.get(Workflow, workflow_id)
        if not (input_dataset and output_dataset and workflow):
            log_msg = ""
            if not input_dataset:
                log_msg += (
                    f"Cannot fetch input_dataset {input_dataset_id} "
                    "from database\n"
                )
            if not output_dataset:
                log_msg += (
                    f"Cannot fetch output_dataset {output_dataset_id} "
                    "from database\n"
                )
            if not workflow:
                log_msg += (
                    f"Cannot fetch workflow {workflow_id} from database\n"
                )
            job.status = JobStatusType.FAILED
            job.end_timestamp = get_timestamp()
            job.log = log_msg
            db_sync.merge(job)
            db_sync.commit()
            db_sync.close()
            return

        # Select backend
        settings = Inject(get_settings)
        FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
        process_workflow = get_process_workflow()

        # Prepare some of process_workflow arguments
        input_paths = input_dataset.paths
        output_path = output_dataset.paths[0]

        # Define and create server-side working folder
        project_id = workflow.project_id
        timestamp_string = get_timestamp().strftime("%Y%m%d_%H%M%S")
        WORKFLOW_DIR = (
            settings.FRACTAL_RUNNER_WORKING_BASE_DIR
            / (
                f"proj_{project_id:07d}_wf_{workflow_id:07d}_job_{job_id:07d}"
                f"_{timestamp_string}"
            )
        ).resolve()

        if WORKFLOW_DIR.exists():
            raise RuntimeError(f"Workflow dir {WORKFLOW_DIR} already exists.")

        # Create WORKFLOW_DIR with 755 permissions
        original_umask = os.umask(0)
        WORKFLOW_DIR.mkdir(parents=True, mode=0o755)
        os.umask(original_umask)

        # Define and create user-side working folder, if needed
        if FRACTAL_RUNNER_BACKEND == "local":
            WORKFLOW_DIR_USER = WORKFLOW_DIR
        elif FRACTAL_RUNNER_BACKEND == "slurm":

            from ._slurm._subprocess_run_as_user import _mkdir_as_user

            WORKFLOW_DIR_USER = (
                Path(user_cache_dir) / f"{WORKFLOW_DIR.name}"
            ).resolve()
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
        log_file_path = WORKFLOW_DIR / "workflow.log"
        logger = set_logger(
            logger_name=logger_name,
            log_file_path=log_file_path,
        )
        logger.info(
            f'Start execution of workflow "{workflow.name}"; '
            f"more logs at {str(log_file_path)}"
        )
        logger.debug(f"fractal_server.__VERSION__: {__VERSION__}")
        logger.debug(f"FRACTAL_RUNNER_BACKEND: {FRACTAL_RUNNER_BACKEND}")
        logger.debug(f"slurm_user: {slurm_user}")
        logger.debug(f"worker_init: {worker_init}")
        logger.debug(f"input metadata: {input_dataset.meta}")
        logger.debug(f"input_paths: {input_paths}")
        logger.debug(f"output_path: {output_path}")
        logger.debug(f"job.id: {job.id}")
        logger.debug(f"job.working_dir: {job.working_dir}")
        logger.debug(f"job.working_dir_user: {job.working_dir_user}")
        logger.debug(f"job.first_task_index: {job.first_task_index}")
        logger.debug(f"job.last_task_index: {job.last_task_index}")
        logger.debug(f'START workflow "{workflow.name}"')

    try:
        # "The Session.close() method does not prevent the Session from being
        # used again. The Session itself does not actually have a distinct
        # “closed” state; it merely means the Session will release all database
        # connections and ORM objects."
        # (https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.Session.close).
        #
        # We close the session before the (possibly long) process_workflow
        # call, to make sure all DB connections are released. The reason why we
        # are not using a context manager within the try block is that we also
        # need access to db_sync in the except branches.
        db_sync = next(DB.get_sync_db())
        db_sync.close()

        output_dataset_meta = await process_workflow(
            workflow=workflow,
            input_paths=input_paths,
            output_path=output_path,
            input_metadata=input_dataset.meta,
            slurm_user=slurm_user,
            user_cache_dir=user_cache_dir,
            workflow_dir=WORKFLOW_DIR,
            workflow_dir_user=WORKFLOW_DIR_USER,
            logger_name=logger_name,
            worker_init=worker_init,
            first_task_index=job.first_task_index,
            last_task_index=job.last_task_index,
        )

        logger.info(
            f'End execution of workflow "{workflow.name}"; '
            f"more logs at {str(log_file_path)}"
        )
        logger.debug(f'END workflow "{workflow.name}"')

        # Replace output_dataset.meta with output_dataset_meta, while handling
        # the history property in a special way (i.e. appending to and
        # existing entry rather than replacing it)
        new_meta = {}
        for key, value in output_dataset_meta.items():
            if key != "history":
                # For non-history keys, replace with new value
                new_meta[key] = value
            else:
                # For history key, append to existing entry
                new_meta[key] = output_dataset.meta.get(key, []) + value
        output_dataset.meta = new_meta
        db_sync.merge(output_dataset)

        # Update job DB entry
        job.status = JobStatusType.DONE
        job.end_timestamp = get_timestamp()
        with log_file_path.open("r") as f:
            logs = f.read()
        job.log = logs
        db_sync.merge(job)
        close_job_logger(logger)
        db_sync.commit()

    except TaskExecutionError as e:

        logger.debug(f'FAILED workflow "{workflow.name}", TaskExecutionError.')
        logger.info(f'Workflow "{workflow.name}" failed (TaskExecutionError).')

        # Assemble output_dataset.meta based on the last successful task, i.e.
        # based on METADATA_FILENAME
        new_meta = assemble_meta_failed_job(job, output_dataset)

        # Assemble new history and assign it to output_dataset.meta
        failed_wftask = db_sync.get(WorkflowTask, e.workflow_task_id)
        new_history = assemble_history_failed_job(
            job,
            output_dataset,
            workflow,
            logger,
            failed_wftask=failed_wftask,
        )
        new_meta["history"] = new_history
        output_dataset.meta = new_meta
        db_sync.merge(output_dataset)

        job.status = JobStatusType.FAILED
        job.end_timestamp = get_timestamp()

        exception_args_string = "\n".join(e.args)
        job.log = (
            f"TASK ERROR:"
            f"Task id: {e.workflow_task_id} ({e.task_name}), "
            f"{e.workflow_task_order=}\n"
            f"TRACEBACK:\n{exception_args_string}"
        )
        db_sync.merge(job)
        close_job_logger(logger)
        db_sync.commit()

    except JobExecutionError as e:

        logger.debug(f'FAILED workflow "{workflow.name}", JobExecutionError.')
        logger.info(f'Workflow "{workflow.name}" failed (JobExecutionError).')

        # Assemble output_dataset.meta based on the last successful task, i.e.
        # based on METADATA_FILENAME
        new_meta = assemble_meta_failed_job(job, output_dataset)

        # Assemble new history and assign it to output_dataset.meta
        new_history = assemble_history_failed_job(
            job,
            output_dataset,
            workflow,
            logger,
        )
        new_meta["history"] = new_history
        output_dataset.meta = new_meta
        db_sync.merge(output_dataset)

        job.status = JobStatusType.FAILED
        job.end_timestamp = get_timestamp()
        error = e.assemble_error()
        job.log = f"JOB ERROR:\nTRACEBACK:\n{error}"
        db_sync.merge(job)
        close_job_logger(logger)
        db_sync.commit()

    except Exception:

        logger.debug(f'FAILED workflow "{workflow.name}", unknown error.')
        logger.info(f'Workflow "{workflow.name}" failed (unkwnon error).')

        current_traceback = traceback.format_exc()

        # Assemble output_dataset.meta based on the last successful task, i.e.
        # based on METADATA_FILENAME
        new_meta = assemble_meta_failed_job(job, output_dataset)

        # Assemble new history and assign it to output_dataset.meta
        new_history = assemble_history_failed_job(
            job,
            output_dataset,
            workflow,
            logger,
        )
        new_meta["history"] = new_history
        output_dataset.meta = new_meta
        db_sync.merge(output_dataset)

        job.status = JobStatusType.FAILED
        job.end_timestamp = get_timestamp()
        job.log = f"UNKNOWN ERROR\nOriginal error: {current_traceback}"
        db_sync.merge(job)
        close_job_logger(logger)
        db_sync.commit()
    finally:
        db_sync.close()
