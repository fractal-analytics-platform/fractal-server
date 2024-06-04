import time

from devtools import debug
from sqlmodel import select

from fractal_server.app.models.v1 import ApplyWorkflow
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.routes.aux._job import _write_shutdown_file
from fractal_server.app.security import get_async_session_context
from fractal_server.config import get_settings
from fractal_server.syringe import Inject

settings = Inject(get_settings)

# If backend is not SLURM, skip this part
# Note that this would need to take place for both V1 and V2 jobs.


async def cleanup_after_shutdown(jobsV1: list[int], jobsV2: list[int]):
    debug("Cleanup function after shutdown")
    try:
        async with get_async_session_context() as session:
            jobsV2_db = (
                (
                    await session.execute(
                        select(JobV2)
                        .where(JobV2.id in jobsV2)
                        .where(JobV2.status == "submitted")
                    )
                )
                .scalars()
                .all()
            )  # untested

            jobsV1_db = (
                (
                    await session.execute(
                        select(ApplyWorkflow)
                        .where(ApplyWorkflow.id in jobsV1)
                        .where(ApplyWorkflow.status == "submitted")
                    )
                )
                .scalars()
                .all()
            )

            for job in jobsV2_db:
                _write_shutdown_file(job=job)

            for job in jobsV1_db:
                _write_shutdown_file(job=job)

            t_start = time.perf_counter()
            while (
                time.perf_counter() - t_start
                < settings.FRACTAL_GRACEFUL_SHUTDOWN_TIME
            ):  # this could be e.g. 30 seconds
                debug("Waiting 3 seconds before checking")
                time.sleep(3)
                jobsV2_db = (
                    (
                        await session.execute(
                            select(JobV2)
                            .where(JobV2.id in jobsV2)
                            .where(JobV2.status == "submitted")
                        )
                    )
                    .scalars()
                    .all()
                )  # untested

                jobsV1_db = (
                    (
                        await session.execute(
                            select(ApplyWorkflow)
                            .where(ApplyWorkflow.id in jobsV1)
                            .where(ApplyWorkflow.status == "submitted")
                        )
                    )
                    .scalars()
                    .all()
                )

                for job in jobsV2_db:
                    _write_shutdown_file(job=job)

                for job in jobsV1_db:
                    _write_shutdown_file(job=job)

                if len(jobsV2_db) == 0 and len(jobsV1_db) == 0:
                    print(
                        (
                            "All jobs associated to this app are "
                            "either done or failed. Exit."
                        )
                    )
                    break
                else:
                    debug(
                        (
                            f"Some jobs are still 'submitted' "
                            f"{jobsV1_db}, {jobsV2_db}"
                        )
                    )
            debug(
                (
                    "Graceful shutdown reached its maximum time, "
                    "but some jobs are still submitted"
                )
            )
            jobsV2_db = (
                (
                    await session.execute(
                        select(JobV2)
                        .where(JobV2.id in jobsV2)
                        .where(JobV2.status == "submitted")
                    )
                )
                .scalars()
                .all()
            )  # untested

            jobsV1_db = (
                (
                    await session.execute(
                        select(ApplyWorkflow)
                        .where(ApplyWorkflow.id in jobsV1)
                        .where(ApplyWorkflow.status == "submitted")
                    )
                )
                .scalars()
                .all()
            )

            for job in jobsV2_db:
                _write_shutdown_file(job=job)

            for job in jobsV1_db:
                _write_shutdown_file(job=job)

            for job in jobsV2_db:
                job.status = "failed"
                if job.log is None:
                    job.log = "Job stopped due to app shutdown\n"
                else:
                    job.log += "Job stopped due to app shutdown\n"
                session.add(job)
                await session.commit()

            for job in jobsV1_db:
                job.status = "failed"
                if job.log is None:
                    job.log = "Job stopped due to app shutdown\n"
                else:
                    job.log += "Job stopped due to app shutdown\n"
                session.add(job)
                await session.commit()
            debug("Exit from shutdown logic")
    except Exception:
        raise ("Connection failed")
