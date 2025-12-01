import random
import sys
import time
from copy import deepcopy
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
from fractal_server.app.schemas.v2 import DatasetImport
from fractal_server.app.schemas.v2 import JobRead
from fractal_server.app.schemas.v2 import ProjectCreate
from fractal_server.app.schemas.v2 import WorkflowCreate
from fractal_server.app.schemas.v2 import WorkflowTaskCreate
from fractal_server.app.schemas.v2.history import HistoryUnitStatus
from scripts.client import FractalClient

random.seed(123112311)


def insert_job(
    project_id: int, workflow_id: int, dataset_id: int, db: Session
) -> JobRead:
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


def insert_history_run(
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
    BATCH_SIZE = 2_000
    if BATCH_SIZE > num_units or (num_units % BATCH_SIZE) != 0:
        BATCH_SIZE = num_units
    num_batches = num_units // BATCH_SIZE

    for ind_batch in range(num_batches):
        db.execute(
            insert(HistoryUnit),
            [
                {
                    "history_run_id": hr_run_id,
                    "logfile": "fake",
                    "status": (
                        HistoryUnitStatus.DONE
                        if i % 2 == 0
                        else HistoryUnitStatus.FAILED
                    ),
                    "zarr_urls": [],
                }
                for i in range(
                    ind_batch * BATCH_SIZE, (ind_batch + 1) * BATCH_SIZE
                )
            ],
        )
        db.commit()

    inserted_ids = (
        db.execute(
            select(HistoryUnit.id).where(
                HistoryUnit.history_run_id == hr_run_id
            )
        )
        .scalars()
        .all()
    )

    return inserted_ids


def bulk_insert_history_image_cache(
    db: Session,
    dataset_id: int,
    workflowtask_id: int,
    history_run_id: int,
    history_unit_ids: list[int],
) -> None:
    num_units = len(history_unit_ids)
    BATCH_SIZE = 2_000
    if BATCH_SIZE > num_units or (num_units % BATCH_SIZE) != 0:
        BATCH_SIZE = num_units
    num_batches = num_units // BATCH_SIZE

    history_unit_ids = deepcopy(history_unit_ids)
    random.shuffle(history_unit_ids)

    for ind_batch in range(num_batches):
        db.execute(
            insert(HistoryImageCache),
            [
                {
                    "zarr_url": f"/run_{history_run_id}/unit_{hu_id}.zarr",
                    "dataset_id": dataset_id,
                    "workflowtask_id": workflowtask_id,
                    "latest_history_unit_id": hu_id,
                }
                for hu_id in history_unit_ids[
                    ind_batch * BATCH_SIZE : (ind_batch + 1) * BATCH_SIZE
                ]
            ],
        )
        db.commit()
    return


if __name__ == "__main__":
    num_clusters = int(sys.argv[1])
    num_units = int(sys.argv[2])

    admin = FractalClient()
    user = _create_user_client(admin, user_identifier="user1")
    proj = user.add_project(ProjectCreate(name="MyProject"))
    working_task = admin.add_working_task()
    for cluster in range(num_clusters):
        ds = user.import_dataset(
            proj.id,
            DatasetImport(
                name=f"MyDataset_{cluster}",
                zarr_dir="/invalid/zarr",
            ),
        )
        wf = user.add_workflow(
            proj.id, WorkflowCreate(name=f"MyWorkflow_{cluster}")
        )
        wftask = user.add_workflowtask(
            proj.id, wf.id, working_task.id, WorkflowTaskCreate()
        )

        with next(get_sync_db()) as db:
            job = insert_job(
                project_id=proj.id, workflow_id=wf.id, dataset_id=ds.id, db=db
            )

            t_start = time.perf_counter()
            hr_run_id = insert_history_run(
                dataset_id=ds.id,
                workflowtask_id=wftask.id,
                task_id=working_task.id,
                job_id=job.id,
                db=db,
            )
            t1 = time.perf_counter()
            unit_ids = bulk_insert_history_units(
                hr_run_id=hr_run_id,
                num_units=num_units,
                db=db,
            )
            t2 = time.perf_counter()
            bulk_insert_history_image_cache(
                dataset_id=ds.id,
                workflowtask_id=wftask.id,
                history_run_id=hr_run_id,
                history_unit_ids=unit_ids,
                db=db,
            )
            t3 = time.perf_counter()
            t_end = time.perf_counter()
            print(
                f"Cluster {cluster} out of {num_clusters} "
                f"- cluster size: {len(unit_ids)} "
                f"- elapsed: {t_end - t_start:.4f} s "
                f"- units: {t2 - t1:.4f} - image caches: {t3 - t2:.4f}"
            )
