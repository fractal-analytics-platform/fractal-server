import time
from logging import Logger

from sqlmodel import select

from fractal_server.app.db import get_async_db
from fractal_server.app.models.v1 import ApplyWorkflow
from fractal_server.app.models.v1.job import JobStatusTypeV1
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2.job import JobStatusTypeV2
from fractal_server.app.routes.aux._job import _write_shutdown_file
from fractal_server.config import get_settings
from fractal_server.syringe import Inject


# If backend is not SLURM, skip this part
# Note that this would need to take place for both V1 and V2 jobs.


async def cleanup_after_shutdown(
    jobsV1: list[int], jobsV2: list[int], logger: Logger
):
    logger.info("Cleanup function after shutdown")
    stm_v2 = (
        select(JobV2)
        .where(JobV2.id.in_(jobsV2))
        .where(JobV2.status == JobStatusTypeV2.SUBMITTED)
    )

    stm_v1 = (
        select(ApplyWorkflow)
        .where(ApplyWorkflow.id.in_(jobsV1))
        .where(ApplyWorkflow.status == JobStatusTypeV1.SUBMITTED)
    )
    try:
        async for session in get_async_db():
            jobsV2_db = (await session.execute(stm_v2)).scalars().all()
            jobsV1_db = (await session.execute(stm_v1)).scalars().all()

            for job in jobsV2_db:
                _write_shutdown_file(job=job)

            for job in jobsV1_db:
                _write_shutdown_file(job=job)

            settings = Inject(get_settings)

            t_start = time.perf_counter()
            while (
                time.perf_counter() - t_start
            ) < settings.FRACTAL_GRACEFUL_SHUTDOWN_TIME:  # 30 seconds
                logger.info("Waiting 3 seconds before checking")
                time.sleep(3)
                jobsV2_db = (await session.execute(stm_v2)).scalars().all()
                jobsV1_db = (await session.execute(stm_v1)).scalars().all()

                if len(jobsV2_db) == 0 and len(jobsV1_db) == 0:
                    logger.info(
                        (
                            "All jobs associated to this app are "
                            "either done or failed. Exit."
                        )
                    )
                    return
                else:
                    logger.info(
                        (
                            f"Some jobs are still 'submitted' "
                            f"{jobsV1_db}, {jobsV2_db}"
                        )
                    )
            logger.info(
                (
                    "Graceful shutdown reached its maximum time, "
                    "but some jobs are still submitted"
                )
            )

            logger.info(jobsV2_db)
            for job in jobsV2_db:
                print("A")
                job.status = "failed"
                if job.log is None:
                    job.log = "Job stopped due to app shutdown\n"
                    print("B")
                else:
                    job.log += "Job stopped due to app shutdown\n"
                    print("C")
                session.add(job)
                print("D")
                await session.commit()

            # jobv2_after = (
            #     await session.execute(select(JobV2))
            # ).scalar_one_or_none()
            #            jobv1_after = (await db.execute(select(ApplyWorkflow))
            # ).scalar_one_or_none()
            #            assert jobv1_after.status == "failed"

            for job in jobsV1_db:
                job.status = "failed"
                if job.log is None:
                    job.log = "Job stopped due to app shutdown\n"
                else:
                    job.log += "Job stopped due to app shutdown\n"
                session.add(job)
                await session.commit()
            logger.info("Exit from shutdown logic")

    except Exception:
        raise ("Connection failed")
