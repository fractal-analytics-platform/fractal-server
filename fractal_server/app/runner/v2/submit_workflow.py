"""
Runner backend subsystem root V2

This module is the single entry point to the runner backend subsystem V2.
Other subsystems should only import this module and not its submodules or
the individual backends.
"""
import os
import traceback
from pathlib import Path

from sqlalchemy.orm import Session as DBSyncSession

from ....config import get_settings
from ....logger import get_logger
from ....logger import reset_logger_handlers
from ....logger import set_logger
from ....ssh._fabric import FractalSSH
from ....syringe import Inject
from ....utils import get_timestamp
from ....zip_tools import _zip_folder_to_file_and_remove
from ...db import DB
from ...models.v2 import DatasetV2
from ...models.v2 import JobV2
from ...models.v2 import WorkflowV2
from ...schemas.v2 import JobStatusTypeV2
from ..exceptions import JobExecutionError
from ..exceptions import TaskExecutionError
from ..filenames import WORKFLOW_LOG_FILENAME
from ._local import process_workflow as local_process_workflow
from ._slurm_ssh import process_workflow as slurm_ssh_process_workflow
from ._slurm_sudo import process_workflow as slurm_sudo_process_workflow
from fractal_server import __VERSION__
from fractal_server.app.models import UserSettings


_backends = {}
_backends["local"] = local_process_workflow
_backends["slurm"] = slurm_sudo_process_workflow
_backends["slurm_ssh"] = slurm_ssh_process_workflow


def fail_job(
    *,
    db: DBSyncSession,
    job: JobV2,
    log_msg: str,
    logger_name: str,
    emit_log: bool = False,
) -> None:
    logger = get_logger(logger_name=logger_name)
    if emit_log:
        logger.error(log_msg)
    reset_logger_handlers(logger)
    job.status = JobStatusTypeV2.FAILED
    job.end_timestamp = get_timestamp()
    job.log = log_msg
    db.merge(job)
    db.commit()
    db.close()
    return


def submit_workflow(
    *,
    workflow_id: int,
    dataset_id: int,
    job_id: int,
    user_id: int,
    user_settings: UserSettings,
    worker_init: str | None = None,
    slurm_user: str | None = None,
    user_cache_dir: str | None = None,
    fractal_ssh: FractalSSH | None = None,
) -> None:
    """
    Prepares a workflow and applies it to a dataset

    This function wraps the process_workflow one, which is different for each
    backend (e.g. local or slurm backend).

    Args:
        workflow_id:
            ID of the workflow being applied
        dataset_id:
            Dataset ID
        job_id:
            Id of the job record which stores the state for the current
            workflow application.
        user_id:
            User ID.
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
    # Declare runner backend and set `process_workflow` function
    settings = Inject(get_settings)
    FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
    logger_name = f"WF{workflow_id}_job{job_id}"
    logger = set_logger(logger_name=logger_name)

    with next(DB.get_sync_db()) as db_sync:
        try:
            job: JobV2 | None = db_sync.get(JobV2, job_id)
            dataset: DatasetV2 | None = db_sync.get(DatasetV2, dataset_id)
            workflow: WorkflowV2 | None = db_sync.get(WorkflowV2, workflow_id)
        except Exception as e:
            logger.error(
                f"Error connecting to the database. Original error: {str(e)}"
            )
            reset_logger_handlers(logger)
            return

        if job is None:
            logger.error(f"JobV2 {job_id} does not exist")
            reset_logger_handlers(logger)
            return
        if dataset is None or workflow is None:
            log_msg = ""
            if not dataset:
                log_msg += f"Cannot fetch dataset {dataset_id} from database\n"
            if not workflow:
                log_msg += (
                    f"Cannot fetch workflow {workflow_id} from database\n"
                )
            fail_job(
                db=db_sync, job=job, log_msg=log_msg, logger_name=logger_name
            )
            return

        # Declare runner backend and set `process_workflow` function
        settings = Inject(get_settings)
        FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
        try:
            process_workflow = _backends[settings.FRACTAL_RUNNER_BACKEND]
        except KeyError as e:
            fail_job(
                db=db_sync,
                job=job,
                log_msg=(
                    f"Invalid {FRACTAL_RUNNER_BACKEND=}.\n"
                    f"Original KeyError: {str(e)}"
                ),
                logger_name=logger_name,
                emit_log=True,
            )
            return

        # Define and create server-side working folder
        WORKFLOW_DIR_LOCAL = Path(job.working_dir)
        if WORKFLOW_DIR_LOCAL.exists():
            fail_job(
                db=db_sync,
                job=job,
                log_msg=f"Workflow dir {WORKFLOW_DIR_LOCAL} already exists.",
                logger_name=logger_name,
                emit_log=True,
            )
            return

        try:
            # Create WORKFLOW_DIR_LOCAL and define WORKFLOW_DIR_REMOTE
            if FRACTAL_RUNNER_BACKEND == "local":
                WORKFLOW_DIR_LOCAL.mkdir(parents=True)
                WORKFLOW_DIR_REMOTE = WORKFLOW_DIR_LOCAL
            elif FRACTAL_RUNNER_BACKEND == "slurm":
                original_umask = os.umask(0)
                WORKFLOW_DIR_LOCAL.mkdir(parents=True, mode=0o755)
                os.umask(original_umask)
                WORKFLOW_DIR_REMOTE = (
                    Path(user_cache_dir) / WORKFLOW_DIR_LOCAL.name
                )
            elif FRACTAL_RUNNER_BACKEND == "slurm_ssh":
                WORKFLOW_DIR_LOCAL.mkdir(parents=True)
                WORKFLOW_DIR_REMOTE = (
                    Path(user_settings.ssh_jobs_dir) / WORKFLOW_DIR_LOCAL.name
                )
            else:
                raise ValueError(
                    "Invalid FRACTAL_RUNNER_BACKEND="
                    f"{settings.FRACTAL_RUNNER_BACKEND}."
                )
        except Exception as e:
            error_type = type(e).__name__
            fail_job(
                db=db_sync,
                job=job,
                log_msg=(
                    f"{error_type} error occurred while creating job folder "
                    f"and subfolders.\nOriginal error: {str(e)}"
                ),
                logger_name=logger_name,
                emit_log=True,
            )
            return

        # After Session.commit() is called, either explicitly or when using a
        # context manager, all objects associated with the Session are expired.
        # https://docs.sqlalchemy.org/en/14/orm/
        #   session_basics.html#opening-and-closing-a-session
        # https://docs.sqlalchemy.org/en/14/orm/
        #   session_state_management.html#refreshing-expiring

        # See issue #928:
        # https://github.com/fractal-analytics-platform/
        #   fractal-server/issues/928

        db_sync.refresh(dataset)
        db_sync.refresh(workflow)
        for wftask in workflow.task_list:
            db_sync.refresh(wftask)

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
        if FRACTAL_RUNNER_BACKEND == "slurm":
            logger.debug(f"slurm_user: {slurm_user}")
            logger.debug(f"slurm_account: {job.slurm_account}")
            logger.debug(f"worker_init: {worker_init}")
        elif FRACTAL_RUNNER_BACKEND == "slurm_ssh":
            logger.debug(f"ssh_user: {user_settings.ssh_username}")
            logger.debug(f"base dir: {user_settings.ssh_tasks_dir}")
            logger.debug(f"slurm_account: {job.slurm_account}")
            logger.debug(f"worker_init: {worker_init}")
        logger.debug(f"job.id: {job.id}")
        logger.debug(f"job.working_dir: {job.working_dir}")
        logger.debug(f"job.working_dir_user: {job.working_dir_user}")
        logger.debug(f"job.first_task_index: {job.first_task_index}")
        logger.debug(f"job.last_task_index: {job.last_task_index}")
        logger.debug(f'START workflow "{workflow.name}"')

    try:
        if FRACTAL_RUNNER_BACKEND == "local":
            process_workflow = local_process_workflow
            backend_specific_kwargs = {}
        elif FRACTAL_RUNNER_BACKEND == "slurm":
            process_workflow = slurm_sudo_process_workflow
            backend_specific_kwargs = dict(
                slurm_user=slurm_user,
                slurm_account=job.slurm_account,
                user_cache_dir=user_cache_dir,
            )
        elif FRACTAL_RUNNER_BACKEND == "slurm_ssh":
            process_workflow = slurm_ssh_process_workflow
            backend_specific_kwargs = dict(
                fractal_ssh=fractal_ssh,
                slurm_account=job.slurm_account,
            )
        else:
            raise RuntimeError(
                f"Invalid runner backend {FRACTAL_RUNNER_BACKEND=}"
            )

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

        process_workflow(
            workflow=workflow,
            dataset=dataset,
            job_id=job_id,
            user_id=user_id,
            workflow_dir_local=WORKFLOW_DIR_LOCAL,
            workflow_dir_remote=WORKFLOW_DIR_REMOTE,
            logger_name=logger_name,
            worker_init=worker_init,
            first_task_index=job.first_task_index,
            last_task_index=job.last_task_index,
            job_attribute_filters=job.attribute_filters,
            job_type_filters=job.type_filters,
            **backend_specific_kwargs,
        )

        logger.info(
            f'End execution of workflow "{workflow.name}"; '
            f"more logs at {str(log_file_path)}"
        )
        logger.debug(f'END workflow "{workflow.name}"')

        # Update job DB entry
        job.status = JobStatusTypeV2.DONE
        job.end_timestamp = get_timestamp()
        with log_file_path.open("r") as f:
            logs = f.read()
        job.log = logs
        db_sync.merge(job)
        db_sync.commit()

    except TaskExecutionError as e:
        logger.debug(f'FAILED workflow "{workflow.name}", TaskExecutionError.')
        logger.info(f'Workflow "{workflow.name}" failed (TaskExecutionError).')

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

        fail_job(
            db=db_sync,
            job=job,
            log_msg=(
                f"JOB ERROR in Fractal job {job.id}:\n"
                f"TRACEBACK:\n{e.assemble_error()}"
            ),
            logger_name=logger_name,
        )

    except Exception:
        logger.debug(f'FAILED workflow "{workflow.name}", unknown error.')
        logger.info(f'Workflow "{workflow.name}" failed (unkwnon error).')

        current_traceback = traceback.format_exc()
        fail_job(
            db=db_sync,
            job=job,
            log_msg=(
                f"UNKNOWN ERROR in Fractal job {job.id}\n"
                f"TRACEBACK:\n{current_traceback}"
            ),
            logger_name=logger_name,
        )

    finally:
        reset_logger_handlers(logger)
        db_sync.close()
        _zip_folder_to_file_and_remove(folder=job.working_dir)
