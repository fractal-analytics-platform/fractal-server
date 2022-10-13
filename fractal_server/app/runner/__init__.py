from sqlalchemy.ext.asyncio import AsyncSession

from ... import __VERSION__
from ..models import ApplyWorkflow
from ..models.security import UserOAuth as User
from ._common import auto_output_dataset  # noqa F401
from ._common import close_job_logger
from ._common import set_job_logger
from ._common import validate_workflow_compatibility  # noqa F401


RUNNER_BACKEND = None


if RUNNER_BACKEND == "PARSL":
    from .parsl import process_workflow
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
    job: ApplyWorkflow,
    user: User,
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

    input_paths = job.input_dataset.paths
    output_path = job.output_dataset.paths[0]

    logger = set_job_logger()

    logger.info(f"fractal_server.__VERSION__: {__VERSION__}")
    logger.info(f"START workflow {job.workflow.name}")

    job.output_dataset.meta = await process_workflow(
        workflow=job.workflow,
        input_paths=input_paths,
        output_path=output_path,
        metadata=job.input_dataset.meta,
        username=user.slurm_username,
        logger=logger,
    )

    logger.info(f'END workflow "{job.workflow.name}"')
    logger.info("Now closing the FileHandler")
    close_job_logger(logger)

    db.add(job.output_dataset)
    await db.commit()
