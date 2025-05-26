from datetime import datetime

from sqlalchemy import insert
from sqlalchemy.orm import Session

from benchmarks.populate_db.populate_db_script import _create_user_client
from benchmarks.populate_db.populate_db_script import create_image_list
from fractal_server.app.db import get_sync_db
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
) -> None:
    sample_workflowtask_dump = {
        "task_type": "sample_workflow",
        "config": {"param1": "value1", "param2": 42},
    }
    sample_task_group_dump = {
        "group_name": "sample_group",
        "settings": {"option1": True, "option2": "test"},
    }

    history_runs = [
        HistoryRun(
            dataset_id=dataset_id,
            workflowtask_id=workflowtask_id,
            job_id=job_id,
            task_id=task_id,
            workflowtask_dump=sample_workflowtask_dump,
            task_group_dump=sample_task_group_dump,
            timestamp_started=datetime.now(),
            status=HistoryUnitStatus.DONE
            if i % 2 == 0
            else HistoryUnitStatus.FAILED,
            num_available_images=i * 10,
        )
        for i in range(10)
    ]

    db.add_all(history_runs)
    db.commit()
    for history_run in history_runs:
        db.refresh(history_run)
    inserted_ids = [history_run.id for history_run in history_runs]
    return inserted_ids


def bulk_insert_history_units(
    hr_run_ids: list[int],
    db: Session,
    num_total_records: int = 10000,
) -> None:
    records_per_run = num_total_records // len(hr_run_ids)

    history_units = []
    for run_id in hr_run_ids:
        for i in range(records_per_run):
            history_units.append(
                {
                    "history_run_id": run_id,
                    "logfile": f"logfile_{run_id}_{i}.txt",
                    "status": HistoryUnitStatus.DONE
                    if i % 3 == 0
                    else HistoryUnitStatus.FAILED,
                    "zarr_urls": [f"zarr://run_{run_id}/file_{i}.zarr"],
                }
            )

        db.execute(
            insert(HistoryUnit),
            history_units,
        )
        db.commit()


if __name__ == "__main__":
    admin = FractalClient()
    user = _create_user_client(admin, user_identifier="user1")
    proj = user.add_project(ProjectCreateV2(name="MyProject"))
    image_list = create_image_list(n_images=100)
    ds = user.import_dataset(
        proj.id,
        DatasetImportV2(
            name="MyDataset",
            zarr_dir="/invalid/zarr",
            images=image_list,
        ),
    )
    wf = user.add_workflow(proj.id, WorkflowCreateV2(name="MyWorkflow"))

    working_task = admin.add_working_task()
    wftask = user.add_workflowtask(
        proj.id, wf.id, working_task.id, WorkflowTaskCreateV2()
    )
    with next(get_sync_db()) as db:
        job = insert_job(
            project_id=proj.id, workflow_id=wf.id, dataset_id=ds.id, db=db
        )
        hr_run_ids = insert_history_runs(
            dataset_id=ds.id,
            workflowtask_id=wftask.id,
            task_id=working_task.id,
            job_id=job.id,
            db=db,
        )
        bulk_insert_history_units(hr_run_ids=hr_run_ids, db=db)
