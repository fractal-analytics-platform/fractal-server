import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

from fractal_server.app.db import get_async_db
from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v2 import DatasetV2
from fractal_server.app.models.v2 import JobV2
from fractal_server.app.models.v2 import ProjectV2
from fractal_server.app.models.v2 import WorkflowV2
from fractal_server.app.schemas.v2 import JobStatusType


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
            name="OLD NAME", project_id=project.id, zarr_dir="/invalid"
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
            status=JobStatusType.SUBMITTED,
        )
        db.add(job)
        await db.commit()
        await db.refresh(job)

        db.expunge_all()
        return dataset.id, job.id


def function1(dataset_id, job_id):
    with next(get_sync_db()) as db_sync:
        dataset = db_sync.get(DatasetV2, dataset_id)
        dataset.name = "NEW NAME"
        db_sync.merge(dataset)
        print(time.perf_counter(), "function1", "merged dataset")
        print(time.perf_counter(), "function1", "now sleep 4")

        time.sleep(4)

        job = db_sync.get(JobV2, job_id)
        job.status = JobStatusType.DONE
        job.log = "asdasdadasda"
        db_sync.merge(job)
        print(time.perf_counter(), "function1", "merged job")

        db_sync.commit()
        print(time.perf_counter(), "function1", "commit")


def function2(dataset_id):
    time.sleep(2)
    with next(get_sync_db()) as db_sync:
        dataset = db_sync.get(DatasetV2, dataset_id)
        print(
            time.perf_counter(),
            "function2",
            "current dataset name:",
            dataset.name,
        )
    time.sleep(4)
    with next(get_sync_db()) as db_sync:
        dataset = db_sync.get(DatasetV2, dataset_id)
        print(
            time.perf_counter(),
            "function2",
            "current dataset name:",
            dataset.name,
        )


async def main():
    dataset_id, job_id = await prepare_data()

    with ThreadPoolExecutor() as executor:
        executor.submit(function1, dataset_id, job_id)
        executor.submit(function2, dataset_id)


if __name__ == "__main__":
    asyncio.run(main())
