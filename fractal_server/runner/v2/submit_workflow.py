"""
Runner backend subsystem root V2

This module is the single entry point to the runner backend subsystem V2.
Other subsystems should only import this module and not its submodules or
the individual backends.
"""
import os
import traceback
from pathlib import Path
from typing import Protocol

from sqlalchemy.orm import Session as DBSyncSession

from ._local import process_workflow as local_process_workflow
from ._slurm_ssh import process_workflow as slurm_ssh_process_workflow
from ._slurm_sudo import process_workflow as slurm_sudo_process_workflow
from fractal_server import __VERSION__
from fractal_server.app.db import DB
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import Profile
from fractal_server.app.models.v2 import Resource
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import JobStatusTypeV2
from fractal_server.app.schemas.v2 import ResourceType
from fractal_server.logger import get_logger
from fractal_server.logger import reset_logger_handlers
from fractal_server.logger import set_logger
from fractal_server.runner.exceptions import JobExecutionError
from fractal_server.runner.filenames import WORKFLOW_LOG_FILENAME
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.types import AttributeFilters
from fractal_server.utils import get_timestamp
from fractal_server.zip_tools import _zip_folder_to_file_and_remove


class ProcessWorkflowType(Protocol):
    def __call__(
        self,
        *,
        workflow: WorkflowV2,
        dataset: DatasetV2,
        workflow_dir_local: Path,
        job_id: int,
        workflow_dir_remote: Path | None,
        first_task_index: int | None,
        last_task_index: int | None,
        logger_name: str,
        job_attribute_filters: AttributeFilters,
        job_type_filters: dict[str, bool],
        user_id: int,
        resource: Resource,
        profile: Profile,
        user_cache_dir: str,
    ) -> None:
        ...


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
    job = db.get(JobV2, job.id)  # refetch, in case it was updated
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
    user_cache_dir: str,
    resource: Resource,
    profile: Profile,
    worker_init: str | None = None,
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
            Cache directory (namely a path where the user can write). For
            `slurm_sudo` backend, this is both a base directory for
            `job.working_dir_user`. For `slurm_sudo` and `slurm_ssh` backends,
            this is used for `user_local_exports`.
        resource:
            Computational resource to be used for this job (e.g. a SLURM
            cluster).
        profile:
           Computational profile to be used for this job.
        fractal_ssh: SSH object, for when `resource.type = "slurm_ssh"`.
    """
    # Declare runner backend and set `process_workflow` function
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

        try:
            # Define local/remote folders, and create local folder
            local_job_dir = Path(job.working_dir)
            remote_job_dir = Path(job.working_dir_user)
            match resource.type:
                case ResourceType.LOCAL:
                    local_job_dir.mkdir(parents=True, exist_ok=False)
                case ResourceType.SLURM_SUDO:
                    original_umask = os.umask(0)
                    local_job_dir.mkdir(
                        parents=True, mode=0o755, exist_ok=False
                    )
                    os.umask(original_umask)
                case ResourceType.SLURM_SSH:
                    local_job_dir.mkdir(parents=True, exist_ok=False)

        except Exception as e:
            error_type = type(e).__name__
            fail_job(
                db=db_sync,
                job=job,
                log_msg=(
                    f"{error_type} error while creating local job folder."
                    f" Original error: {str(e)}"
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
        log_file_path = local_job_dir / WORKFLOW_LOG_FILENAME
        logger = set_logger(
            logger_name=logger_name,
            log_file_path=log_file_path,
        )
        logger.info(
            f'Start execution of workflow "{workflow.name}"; '
            f"more logs at {str(log_file_path)}"
        )
        logger.debug(f"fractal_server.__VERSION__: {__VERSION__}")
        logger.debug(f"Resource name: {resource.name}")
        logger.debug(f"Profile name: {profile.name}")
        logger.debug(f"Username: {profile.username}")
        if resource.type in [ResourceType.SLURM_SUDO, ResourceType.SLURM_SSH]:
            logger.debug(f"slurm_account: {job.slurm_account}")
            logger.debug(f"worker_init: {worker_init}")
        logger.debug(f"job.id: {job.id}")
        logger.debug(f"job.working_dir: {job.working_dir}")
        logger.debug(f"job.working_dir_user: {job.working_dir_user}")
        logger.debug(f"job.first_task_index: {job.first_task_index}")
        logger.debug(f"job.last_task_index: {job.last_task_index}")
        logger.debug(f'START workflow "{workflow.name}"')
        job_working_dir = job.working_dir

    try:
        process_workflow: ProcessWorkflowType
        match resource.type:
            case ResourceType.LOCAL:
                process_workflow = local_process_workflow
                backend_specific_kwargs = {}
            case ResourceType.SLURM_SUDO:
                process_workflow = slurm_sudo_process_workflow
                backend_specific_kwargs = dict(
                    slurm_account=job.slurm_account,
                )
            case ResourceType.SLURM_SSH:
                process_workflow = slurm_ssh_process_workflow
                backend_specific_kwargs = dict(
                    fractal_ssh=fractal_ssh,
                    slurm_account=job.slurm_account,
                )

        process_workflow(
            workflow=workflow,
            dataset=dataset,
            job_id=job_id,
            user_id=user_id,
            workflow_dir_local=local_job_dir,
            workflow_dir_remote=remote_job_dir,
            logger_name=logger_name,
            worker_init=worker_init,
            first_task_index=job.first_task_index,
            last_task_index=job.last_task_index,
            job_attribute_filters=job.attribute_filters,
            job_type_filters=job.type_filters,
            resource=resource,
            profile=profile,
            user_cache_dir=user_cache_dir,
            **backend_specific_kwargs,
        )

        logger.info(
            f'End execution of workflow "{workflow.name}"; '
            f"more logs at {str(log_file_path)}"
        )
        logger.debug(f'END workflow "{workflow.name}"')

        # Update job DB entry
        with next(DB.get_sync_db()) as db_sync:
            job = db_sync.get(JobV2, job_id)
            job.status = JobStatusTypeV2.DONE
            job.end_timestamp = get_timestamp()
            with log_file_path.open("r") as f:
                logs = f.read()
            job.log = logs
            db_sync.merge(job)
            db_sync.commit()

    except JobExecutionError as e:
        logger.debug(f'FAILED workflow "{workflow.name}", JobExecutionError.')
        logger.info(f'Workflow "{workflow.name}" failed (JobExecutionError).')
        with next(DB.get_sync_db()) as db_sync:
            job = db_sync.get(JobV2, job_id)
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
        with next(DB.get_sync_db()) as db_sync:
            job = db_sync.get(JobV2, job_id)
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
        _zip_folder_to_file_and_remove(folder=job_working_dir)
