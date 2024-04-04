"""
Runner backend subsystem root V2

This module is the single entry point to the runner backend subsystem V2.
Other subystems should only import this module and not its submodules or
the individual backends.
"""
import os
import traceback
from pathlib import Path
from typing import Optional

from sqlalchemy.orm.attributes import flag_modified

from ....config import get_settings
from ....logger import close_logger
from ....logger import set_logger
from ....syringe import Inject
from ....utils import get_timestamp
from ...db import DB
from ...models.v2 import DatasetV2
from ...models.v2 import JobV2
from ...models.v2 import WorkflowTaskV2
from ...models.v2 import WorkflowV2
from ...schemas.v2 import JobStatusTypeV2
from ..exceptions import JobExecutionError
from ..exceptions import TaskExecutionError
from ..filenames import WORKFLOW_LOG_FILENAME
from ._local import process_workflow as local_process_workflow
from ._slurm import process_workflow as slurm_process_workflow
from .handle_failed_job import assemble_filters_failed_job
from .handle_failed_job import assemble_history_failed_job
from .handle_failed_job import assemble_images_failed_job
from fractal_server import __VERSION__

_backends = {}
_backends["local"] = local_process_workflow
_backends["slurm"] = slurm_process_workflow


async def submit_workflow(
    *,
    workflow_id: int,
    dataset_id: int,
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
        dataset_id:
            Dataset ID
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

    # Declare runner backend and set `process_workflow` function
    settings = Inject(get_settings)
    FRACTAL_RUNNER_BACKEND = settings.FRACTAL_RUNNER_BACKEND
    if FRACTAL_RUNNER_BACKEND == "local":
        process_workflow = local_process_workflow
    elif FRACTAL_RUNNER_BACKEND == "slurm":
        process_workflow = slurm_process_workflow
    else:
        raise RuntimeError(f"Invalid runner backend {FRACTAL_RUNNER_BACKEND=}")

    with next(DB.get_sync_db()) as db_sync:

        job: JobV2 = db_sync.get(JobV2, job_id)
        if not job:
            raise ValueError(f"Cannot fetch job {job_id} from database")

        dataset: DatasetV2 = db_sync.get(DatasetV2, dataset_id)
        workflow: WorkflowV2 = db_sync.get(WorkflowV2, workflow_id)
        if not (dataset and workflow):
            log_msg = ""
            if not dataset:
                log_msg += f"Cannot fetch dataset {dataset_id} from database\n"
            if not workflow:
                log_msg += (
                    f"Cannot fetch workflow {workflow_id} from database\n"
                )
            job.status = JobStatusTypeV2.FAILED
            job.end_timestamp = get_timestamp()
            job.log = log_msg
            db_sync.merge(job)
            db_sync.commit()
            db_sync.close()
            return

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

            from ..executors.slurm._subprocess_run_as_user import (
                _mkdir_as_user,
            )

            WORKFLOW_DIR_USER = (
                Path(user_cache_dir) / f"{WORKFLOW_DIR.name}"
            ).resolve()
            _mkdir_as_user(folder=str(WORKFLOW_DIR_USER), user=slurm_user)
        else:
            raise ValueError(f"{FRACTAL_RUNNER_BACKEND=} not supported")

        # Update db
        job.working_dir = WORKFLOW_DIR.as_posix()
        job.working_dir_user = WORKFLOW_DIR_USER.as_posix()
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

        db_sync.refresh(dataset)
        db_sync.refresh(workflow)

        # Write logs
        logger_name = f"WF{workflow_id}_job{job_id}"
        log_file_path = WORKFLOW_DIR / WORKFLOW_LOG_FILENAME
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

        new_dataset_attributes = await process_workflow(
            workflow=workflow,
            dataset=dataset,
            slurm_user=slurm_user,
            slurm_account=job.slurm_account,
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

        # Update dataset attributes, in case of successful execution
        dataset.history.extend(new_dataset_attributes["history"])
        dataset.filters = new_dataset_attributes["filters"]
        dataset.images = new_dataset_attributes["images"]
        for attribute_name in ["filters", "history", "images"]:
            flag_modified(dataset, attribute_name)
        db_sync.merge(dataset)

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

        # Read dataset attributes produced by the last successful task, and
        # update the DB dataset accordingly
        failed_wftask = db_sync.get(WorkflowTaskV2, e.workflow_task_id)
        dataset.history = assemble_history_failed_job(
            job,
            dataset,
            workflow,
            logger,
            failed_wftask=failed_wftask,
        )
        latest_filters = assemble_filters_failed_job(job)
        if latest_filters is not None:
            dataset.filters = latest_filters
        latest_images = assemble_images_failed_job(job)
        if latest_images is not None:
            dataset.images = latest_images
        db_sync.merge(dataset)

        job.status = JobStatusTypeV2.FAILED
        job.end_timestamp = get_timestamp()

        exception_args_string = "\n".join(e.args)
        job.log = (
            f"TASK ERROR: "
            f"Task name: {e.task_name}, "
            f"position in Workflow: {e.workflow_task_order}\n"
            f"TRACEBACK:\n{exception_args_string}"
        )
        db_sync.merge(job)
        db_sync.commit()

    except JobExecutionError as e:

        logger.debug(f'FAILED workflow "{workflow.name}", JobExecutionError.')
        logger.info(f'Workflow "{workflow.name}" failed (JobExecutionError).')

        # Read dataset attributes produced by the last successful task, and
        # update the DB dataset accordingly
        dataset.history = assemble_history_failed_job(
            job,
            dataset,
            workflow,
            logger,
        )
        latest_filters = assemble_filters_failed_job(job)
        if latest_filters is not None:
            dataset.filters = latest_filters
        latest_images = assemble_images_failed_job(job)
        if latest_images is not None:
            dataset.images = latest_images
        db_sync.merge(dataset)

        job.status = JobStatusTypeV2.FAILED
        job.end_timestamp = get_timestamp()
        error = e.assemble_error()
        job.log = f"JOB ERROR in Fractal job {job.id}:\nTRACEBACK:\n{error}"
        db_sync.merge(job)
        db_sync.commit()

    except Exception:

        logger.debug(f'FAILED workflow "{workflow.name}", unknown error.')
        logger.info(f'Workflow "{workflow.name}" failed (unkwnon error).')

        current_traceback = traceback.format_exc()

        # Read dataset attributes produced by the last successful task, and
        # update the DB dataset accordingly
        dataset.history = assemble_history_failed_job(
            job,
            dataset,
            workflow,
            logger,
        )
        latest_filters = assemble_filters_failed_job(job)
        if latest_filters is not None:
            dataset.filters = latest_filters
        latest_images = assemble_images_failed_job(job)
        if latest_images is not None:
            dataset.images = latest_images
        db_sync.merge(dataset)

        job.status = JobStatusTypeV2.FAILED
        job.end_timestamp = get_timestamp()
        job.log = (
            f"UNKNOWN ERROR in Fractal job {job.id}\n"
            f"TRACEBACK:\n{current_traceback}"
        )
        db_sync.merge(job)
        db_sync.commit()
    finally:
        close_logger(logger)
        db_sync.close()
