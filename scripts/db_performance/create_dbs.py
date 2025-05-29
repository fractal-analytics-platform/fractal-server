from datetime import datetime

from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy.orm import Session

from benchmarks.populate_db.populate_db_script import _create_user_client
from fractal_server.app.db import get_sync_db
from fractal_server.app.models import HistoryImageCache
from fractal_server.app.models import HistoryRun
from fractal_server.app.models import HistoryUnit
from fractal_server.app.models import JobV2
from fractal_server.app.schemas.v2 import DatasetImportV2
from fractal_server.app.schemas.v2 import JobReadV2
from fractal_server.app.schemas.v2 import ProjectCreateV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from fractal_server.app.schemas.v2.history import HistoryUnitStatus
from scripts.client import FractalClient


def insert_job(
    project_id: int, workflow_id: int, dataset_id: int, db: Session
) -> JobReadV2:
    job = JobV2(
        project_id=project_id,
        workflow_id=workflow_id,
        dataset_id=dataset_id,
        user_email="email",
        dataset_dump={"dataset_dump": "dataset"},
        workflow_dump={"workflow_dump": "wf"},
        project_dump={"project_dump": "prj"},
        first_task_index=0,
        last_task_index=1,
    )

    db.add(job)
    db.commit()
    db.refresh(job)
    return job


def insert_history_runs(
    dataset_id: int,
    workflowtask_id: int,
    task_id: int,
    job_id: int,
    db: Session,
) -> list[int]:
    sample_workflowtask_dump = {
        "task_type": "sample_workflow",
        "config": {"param1": "value1", "param2": 42},
    }
    sample_task_group_dump = {
        "group_name": "sample_group",
        "settings": {"option1": True, "option2": "test"},
    }

    history_run = HistoryRun(
        dataset_id=dataset_id,
        workflowtask_id=workflowtask_id,
        job_id=job_id,
        task_id=task_id,
        workflowtask_dump=sample_workflowtask_dump,
        task_group_dump=sample_task_group_dump,
        timestamp_started=datetime.now(),
        status="mock-status",
        num_available_images=1,
    )

    db.add(history_run)
    db.commit()
    db.refresh(history_run)
    return history_run.id


def bulk_insert_history_units(
    hr_run_id: int,
    num_units: int,
    db: Session,
) -> list[int]:
    BATCH_SIZE = 1_000
    if (num_units % BATCH_SIZE) != 0:
        raise ValueError(num_units, BATCH_SIZE)
    num_batches = num_units // num_units

    for ind_batch in range(num_batches):
        history_units = [
            {
                "history_run_id": hr_run_id,
                "logfile": f"logfile_{hr_run_id}_{i}.txt",
                "status": HistoryUnitStatus.DONE
                if i % 2 == 0
                else HistoryUnitStatus.FAILED,
                "zarr_urls": [f"zarr://run_{hr_run_id}/file_{i}.zarr"],
            }
            for i in range(
                ind_batch * BATCH_SIZE, (ind_batch + 1) * BATCH_SIZE
            )
        ]
        db.execute(insert(HistoryUnit), history_units)
        db.commit()
    inserted_ids = [
        hu_id[0]
        for hu_id in db.execute(
            select(HistoryUnit.id).where(
                HistoryUnit.history_run_id == hr_run_id
            )
        ).all()
    ]
    zarr_urls = [
        hu_id[0]
        for hu_id in db.execute(
            select(HistoryUnit.zarr_urls).where(
                HistoryUnit.history_run_id == hr_run_id
            )
        ).all()
    ]
    return dict(h_units=inserted_ids, zarr_urls=zarr_urls)


def bulk_insert_history_image_cache(
    db: Session,
    dataset_id: int,
    workflowtask_id: int,
    history_unit_ids: list[int],
    zarr_urls: list[str],
) -> list[int]:

    BATCH_SIZE = 1_000
    num_units = len(history_unit_ids)
    if (num_units % BATCH_SIZE) != 0:
        raise ValueError(num_units, BATCH_SIZE)
    num_batches = num_units // num_units

    for ind_batch in range(num_batches):
        history_image_caches = [
            {
                "zarr_url": zarr_urls[ind_batch * BATCH_SIZE + ind_internal],
                "dataset_id": dataset_id,
                "workflowtask_id": workflowtask_id,
                "latest_history_unit_id": hu_id,
            }
            for ind_internal, hu_id in enumerate(
                history_unit_ids[
                    ind_batch * BATCH_SIZE : (ind_batch + 1) * BATCH_SIZE
                ]
            )
        ]
        db.execute(
            insert(HistoryImageCache),
            history_image_caches,
        )
        db.commit()

    res = db.execute(select(HistoryImageCache.zarr_url))
    inserted_hic = [hic_zarr_url[0] for hic_zarr_url in res.all()]

    return inserted_hic


if __name__ == "__main__":
    num_clusters = 1000
    num_units = 1000

    admin = FractalClient()
    user = _create_user_client(admin, user_identifier="user1")
    proj = user.add_project(ProjectCreateV2(name="MyProject"))
    working_task = admin.add_working_task()
    for cluster in range(num_clusters):
        ds = user.import_dataset(
            proj.id,
            DatasetImportV2(
                name=f"MyDataset_{cluster}",
                zarr_dir="/invalid/zarr",
            ),
        )
        wf = user.add_workflow(
            proj.id, WorkflowCreateV2(name=f"MyWorkflow_{cluster}")
        )
        wftask = user.add_workflowtask(
            proj.id, wf.id, working_task.id, WorkflowTaskCreateV2()
        )

        with next(get_sync_db()) as db:
            job = insert_job(
                project_id=proj.id, workflow_id=wf.id, dataset_id=ds.id, db=db
            )
            hr_run_id = insert_history_runs(
                dataset_id=ds.id,
                workflowtask_id=wftask.id,
                task_id=working_task.id,
                job_id=job.id,
                db=db,
            )
            dict_units = bulk_insert_history_units(
                hr_run_id=hr_run_id, num_units=num_units, db=db
            )
            print(len(dict_units["h_units"]))
            hic_ids = bulk_insert_history_image_cache(
                dataset_id=ds.id,
                workflowtask_id=wftask.id,
                history_unit_ids=dict_units["h_units"],
                zarr_urls=dict_units["zarr_urls"],
                db=db,
            )
