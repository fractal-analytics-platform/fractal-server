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
Runner backend subsystem root

This module is the single entry point to the runner backend subsystem. Other
subystems should only import this module and not its submodules or the
individual backends.
"""
import os
import traceback
from pathlib import Path
from typing import Optional

from sqlalchemy.orm import Session as DBSyncSession

from ....logger import get_logger
from ....logger import reset_logger_handlers
from ....logger import set_logger
from ....syringe import Inject
from ....utils import get_timestamp
from ...db import DB
from ...models.v1 import ApplyWorkflow
from ...models.v1 import Dataset
from ...models.v1 import Workflow
from ...models.v1 import WorkflowTask
from ...schemas.v1 import JobStatusTypeV1
from ..exceptions import JobExecutionError
from ..exceptions import TaskExecutionError
from ..executors.slurm.sudo._subprocess_run_as_user import (
    _mkdir_as_user,
)
from ..filenames import WORKFLOW_LOG_FILENAME
from ..task_files import task_subfolder_name
from ._local import process_workflow as local_process_workflow
from ._slurm import process_workflow as slurm_process_workflow
from .common import close_job_logger
from .common import validate_workflow_compatibility  # noqa: F401
from .handle_failed_job import assemble_history_failed_job
from .handle_failed_job import assemble_meta_failed_job
from fractal_server import __VERSION__
from fractal_server.config import get_settings


_backends = {}
_backends["local"] = local_process_workflow
_backends["slurm"] = slurm_process_workflow


def fail_job(
    *,
    db: DBSyncSession,
    job: ApplyWorkflow,
    log_msg: str,
    logger_name: str,
    emit_log: bool = False,
) -> None:
    logger = get_logger(logger_name=logger_name)
    if emit_log:
        logger.error(log_msg)
    reset_logger_handlers(logger)
    job.status = JobStatusTypeV1.FAILED
    job.end_timestamp = get_timestamp()
    job.log = log_msg
    db.merge(job)
    db.commit()
    db.close()
    return


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

    logger_name = f"WF{workflow_id}_job{job_id}"
    logger = set_logger(logger_name=logger_name)

    with next(DB.get_sync_db()) as db_sync:

        job: ApplyWorkflow = db_sync.get(ApplyWorkflow, job_id)
        if not job:
            logger.error(f"ApplyWorkflow {job_id} does not exist")
            return

        settings = Inject(get_settings)
        FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
        if FRACTAL_RUNNER_BACKEND == "local":
            process_workflow = local_process_workflow
        elif FRACTAL_RUNNER_BACKEND == "slurm":
            process_workflow = slurm_process_workflow
        else:

            if FRACTAL_RUNNER_BACKEND == "local_experimental":
                log_msg = (
                    f"{FRACTAL_RUNNER_BACKEND=} is not available for v1 jobs."
                )
            else:
                log_msg = f"Invalid {FRACTAL_RUNNER_BACKEND=}"

            fail_job(
                job=job,
                db=db_sync,
                log_msg=log_msg,
                logger_name=logger_name,
                emit_log=True,
            )
            return

        # Declare runner backend and set `process_workflow` function

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
            fail_job(
                db=db_sync, job=job, log_msg=log_msg, logger_name=logger_name
            )
            return

        # Prepare some of process_workflow arguments
        input_paths = input_dataset.paths
        output_path = output_dataset.paths[0]

        # Define and create server-side working folder
        project_id = workflow.project_id
        timestamp_string = get_timestamp().strftime("%Y%m%d_%H%M%S")
        WORKFLOW_DIR_LOCAL = settings.FRACTAL_RUNNER_WORKING_BASE_DIR / (
            f"proj_{project_id:07d}_wf_{workflow_id:07d}_job_{job_id:07d}"
            f"_{timestamp_string}"
        )

        if WORKFLOW_DIR_LOCAL.exists():
            fail_job(
                db=db_sync,
                job=job,
                log_msg=f"Workflow dir {WORKFLOW_DIR_LOCAL} already exists.",
                logger_name=logger_name,
                emit_log=True,
            )
            return

        # Create WORKFLOW_DIR
        original_umask = os.umask(0)
        WORKFLOW_DIR_LOCAL.mkdir(parents=True, mode=0o755)
        os.umask(original_umask)

        # Define and create WORKFLOW_DIR_REMOTE
        if FRACTAL_RUNNER_BACKEND == "local":
            WORKFLOW_DIR_REMOTE = WORKFLOW_DIR_LOCAL
        elif FRACTAL_RUNNER_BACKEND == "slurm":
            WORKFLOW_DIR_REMOTE = (
                Path(user_cache_dir) / WORKFLOW_DIR_LOCAL.name
            )
            _mkdir_as_user(folder=str(WORKFLOW_DIR_REMOTE), user=slurm_user)

        # Create all tasks subfolders
        for order in range(job.first_task_index, job.last_task_index + 1):
            subfolder_name = task_subfolder_name(
                order=order,
                task_name=workflow.task_list[order].task.name,
            )
            original_umask = os.umask(0)
            (WORKFLOW_DIR_LOCAL / subfolder_name).mkdir(mode=0o755)
            os.umask(original_umask)
            if FRACTAL_RUNNER_BACKEND == "slurm":
                _mkdir_as_user(
                    folder=str(WORKFLOW_DIR_REMOTE / subfolder_name),
                    user=slurm_user,
                )

        # Update db
        job.working_dir = WORKFLOW_DIR_LOCAL.as_posix()
        job.working_dir_user = WORKFLOW_DIR_REMOTE.as_posix()
        db_sync.merge(job)
        db_sync.commit()

        # After Session.commit() is called, either explicitly or when using a
        # context manager, all objects associated with the Session are expired.
        # https://docs.sqlalchemy.org/en/14/orm/
        #   session_basics.html#opening-and-closing-a-session
        # https://docs.sqlalchemy.org/en/14/orm/
        #   session_state_management.html#refreshing-expiring

        # See issue #928:
        # https://github.com/fractal-analytics-platform/
        #   fractal-server/issues/928

        db_sync.refresh(input_dataset)
        db_sync.refresh(output_dataset)
        db_sync.refresh(workflow)

        # Write logs
        log_file_path = WORKFLOW_DIR_LOCAL / WORKFLOW_LOG_FILENAME
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
        logger.debug(f"slurm_account: {job.slurm_account}")
        logger.debug(f"worker_init: {worker_init}")
        logger.debug(f"input metadata keys: {list(input_dataset.meta.keys())}")
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

        output_dataset_meta_hist = await process_workflow(
            workflow=workflow,
            input_paths=input_paths,
            output_path=output_path,
            input_metadata=input_dataset.meta,
            input_history=input_dataset.history,
            slurm_user=slurm_user,
            slurm_account=job.slurm_account,
            user_cache_dir=user_cache_dir,
            workflow_dir_local=WORKFLOW_DIR_LOCAL,
            workflow_dir_remote=WORKFLOW_DIR_REMOTE,
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

        # Replace output_dataset.meta and output_dataset.history with their
        # up-to-date versions, obtained within process_workflow
        output_dataset.history = output_dataset_meta_hist.pop("history")
        output_dataset.meta = output_dataset_meta_hist.pop("metadata")

        db_sync.merge(output_dataset)

        # Update job DB entry
        job.status = JobStatusTypeV1.DONE
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
        output_dataset.meta = assemble_meta_failed_job(job, output_dataset)

        # Assemble new history and assign it to output_dataset.meta
        failed_wftask = db_sync.get(WorkflowTask, e.workflow_task_id)
        output_dataset.history = assemble_history_failed_job(
            job,
            output_dataset,
            workflow,
            logger,
            failed_wftask=failed_wftask,
        )

        db_sync.merge(output_dataset)

        exception_args_string = "\n".join(e.args)
        log_msg = (
            f"TASK ERROR: "
            f"Task name: {e.task_name}, "
            f"position in Workflow: {e.workflow_task_order}\n"
            f"TRACEBACK:\n{exception_args_string}"
        )
        fail_job(db=db_sync, job=job, log_msg=log_msg, logger_name=logger_name)

    except JobExecutionError as e:

        logger.debug(f'FAILED workflow "{workflow.name}", JobExecutionError.')
        logger.info(f'Workflow "{workflow.name}" failed (JobExecutionError).')

        # Assemble output_dataset.meta based on the last successful task, i.e.
        # based on METADATA_FILENAME
        output_dataset.meta = assemble_meta_failed_job(job, output_dataset)

        # Assemble new history and assign it to output_dataset.meta
        output_dataset.history = assemble_history_failed_job(
            job,
            output_dataset,
            workflow,
            logger,
        )

        db_sync.merge(output_dataset)
        error = e.assemble_error()
        fail_job(
            db=db_sync,
            job=job,
            log_msg=f"JOB ERROR in Fractal job {job.id}:\nTRACEBACK:\n{error}",
            logger_name=logger_name,
        )

    except Exception:

        logger.debug(f'FAILED workflow "{workflow.name}", unknown error.')
        logger.info(f'Workflow "{workflow.name}" failed (unkwnon error).')

        current_traceback = traceback.format_exc()

        # Assemble output_dataset.meta based on the last successful task, i.e.
        # based on METADATA_FILENAME
        output_dataset.meta = assemble_meta_failed_job(job, output_dataset)

        # Assemble new history and assign it to output_dataset.meta
        output_dataset.history = assemble_history_failed_job(
            job,
            output_dataset,
            workflow,
            logger,
        )

        db_sync.merge(output_dataset)

        log_msg = (
            f"UNKNOWN ERROR in Fractal job {job.id}\n"
            f"TRACEBACK:\n{current_traceback}"
        )
        fail_job(db=db_sync, job=job, log_msg=log_msg, logger_name=logger_name)

    finally:
        db_sync.close()
        reset_logger_handlers(logger)
