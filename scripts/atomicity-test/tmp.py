import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from devtools import debug

from fractal_server.app.db import get_async_db
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import JobStatusTypeV2


async def prepare_data():
    async for db in get_async_db():
        project = ProjectV2(name="my project")
        db.add(project)
        await db.commit()
        await db.refresh(project)

        workflow = WorkflowV2(name="my workflow", project_id=project.id)
        db.add(workflow)
        await db.commit()
        await db.refresh(workflow)

        dataset = DatasetV2(
            name="my dataset", project_id=project.id, zarr_dir="/tmp/invalid"
        )
        db.add(dataset)
        await db.commit()
        await db.refresh(dataset)

        job = JobV2(
            project_id=project.id,
            dataset_id=dataset.id,
            workflow_id=workflow.id,
            project_dump={},
            dataset_dump={},
            workflow_dump={},
            user_email="a@example.org",
            first_task_index=0,
            last_task_index=0,
            status=JobStatusTypeV2.SUBMITTED,
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)

        db.expunge_all()
        return dataset.id, job.id


def function1(dataset_id, job_id):
    with next(get_sync_db) as db_sync:
        dataset = db_sync.get(DatasetV2, dataset_id)
        job = db_sync.get(JobV2, job_id)

        dataset.name = "New name"
        db_sync.merge(dataset)

        time.sleep(10)

        # Update job DB entry
        job.status = JobStatusTypeV2.DONE
        job.log = "asdasdadasda"
        db_sync.merge(job)
        db_sync.commit()


def function2(dataset_id, job_id):
    with next(get_sync_db) as db_sync:
        time.sleep(5)
        dataset = db_sync.get(DatasetV2, dataset_id)
        debug("f2", dataset)


async def main():
    dataset_id, job_id = await prepare_data()

    with ThreadPoolExecutor() as executor:
        executor.submit(function1, dataset_id, job_id)
        # executor.submit(function2, dataset_id, job_id)


if __name__ == "__main__":
    asyncio.run(main())
