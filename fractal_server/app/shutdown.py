import time

from sqlmodel import select

from fractal_server.app.db import get_async_db
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2.job import JobStatusTypeV2
from fractal_server.app.routes.aux._job import _write_shutdown_file
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.syringe import Inject


async def cleanup_after_shutdown(*, jobsV2: list[int], logger_name: str):
    settings = Inject(get_settings)
    logger = get_logger(logger_name)
    logger.info("Cleanup function after shutdown")
    stm_objects = (
        select(JobV2)
        .where(JobV2.id.in_(jobsV2))
        .where(JobV2.status == JobStatusTypeV2.SUBMITTED)
    )
    stm_ids = (
        select(JobV2.id)
        .where(JobV2.id.in_(jobsV2))
        .where(JobV2.status == JobStatusTypeV2.SUBMITTED)
    )

    async for session in get_async_db():
        # Write shutdown file for all jobs
        jobs = (await session.execute(stm_objects)).scalars().all()
        for job in jobs:
            _write_shutdown_file(job=job)

        # Wait for completion of all job - with a timeout
        interval = settings.FRACTAL_GRACEFUL_SHUTDOWN_TIME / 20
        t_start = time.perf_counter()
        while (
            time.perf_counter() - t_start
        ) <= settings.FRACTAL_GRACEFUL_SHUTDOWN_TIME:
            job_ids = (await session.execute(stm_ids)).scalars().all()
            if len(job_ids) == 0:
                logger.info("All jobs are either done or failed. Exit.")
                return
            else:
                logger.info(f"Some jobs are still 'submitted': {job_ids=}")
                logger.info(f"Wait {interval:.4f} seconds before next check.")
                time.sleep(interval)
        logger.info(
            "Graceful shutdown reached its maximum time, "
            "but some jobs are still submitted."
        )

        # Mark jobs as failed and update their logs.
        jobs = (await session.execute(stm_objects)).scalars().all()
        for job in jobs:
            job.status = "failed"
            job.log = (job.log or "") + "\nJob stopped due to app shutdown\n"
            session.add(job)
        await session.commit()

        logger.info("Exit from shutdown logic")
