import logging
import os
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from ... import __VERSION__
from ...config_runner import settings
from ...utils import async_wrap
from ..models import Dataset
from ..models import Task
from ._common import auto_output_dataset  # noqa: F401
from ._common import close_job_logger
from ._common import set_job_logger
from ._common import validate_workflow_compatibility  # noqa: F401


RUNNER_BACKEND = "PARSL"


if RUNNER_BACKEND == "PARSL":
    from .parsl import _process_workflow, get_app_future_result
elif RUNNER_BACKEND == "process":
    from .process import submit_workflow
else:

    def no_function(*args, **kwarsg):
        raise NotImplementedError(
            f"Runner backend {RUNNER_BACKEND} not implemented"
        )

    submit_workflow = no_function


async def submit_workflow(
    *,
    db: AsyncSession,
    workflow: Task,
    input_dataset: Dataset,
    output_dataset: Dataset,
    job_id: int,
    username: str = None,
    worker_init: str = None,
):
    """
    Prepares a workflow and applies it to a dataset

    Arguments
    ---------
    db: (AsyncSession):
        Asynchronous database session
    output_dataset (Dataset | str) :
        the destination dataset of the workflow. If not provided, overwriting
        of the input dataset is implied and an error is raised if the dataset
        is in read only mode. If a string is passed and the dataset does not
        exist, a new dataset with that name is created and within it a new
        resource with the same name.
    """

    input_paths = input_dataset.paths
    output_path = output_dataset.paths[0]

    workflow_id = workflow.id

    RUNNER_LOG_DIR = settings.RUNNER_LOG_DIR
    if not os.path.isdir(RUNNER_LOG_DIR):
        os.mkdir(RUNNER_LOG_DIR)
    workflow_log_dir = f"{RUNNER_LOG_DIR}/workflow_{workflow_id:06d}"
    if not os.path.isdir(workflow_log_dir):
        os.mkdir(workflow_log_dir)

    logger = set_job_logger(
        logger_name=f"WF{workflow_id}",
        log_file_path=Path(f"{workflow_log_dir}/workflow.log"),
        level=logging.INFO,
        formatter=logging.Formatter("%(asctime)s; %(levelname)s; %(message)s"),
    )

    logger.info(f"fractal_server.__VERSION__: {__VERSION__}")
    logger.info(f"START workflow {workflow.name}")

    logger.info(f"{input_paths=}")
    logger.info(f"{output_path=}")

    final_metadata, dfk = _process_workflow(
        task=workflow,
        input_paths=input_paths,
        output_path=output_path,
        metadata=input_dataset.meta,
        username=username,
        worker_init=worker_init,
    )
    logger.info("Definition of app futures complete, now start execution. ")
    output_dataset.meta = await async_wrap(get_app_future_result)(
        app_future=final_metadata
    )
    logger.info(f'END workflow "{workflow.name}"')

    dfk.cleanup()

    close_job_logger(logger)
    db.add(output_dataset)

    await db.commit()
