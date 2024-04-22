# Add `fractal_client.py` parent directory to `sys.path`
import sys
from pathlib import Path

sys.path.append((Path(__file__).parents[1] / "scripts/populate_db").as_posix())

from fractal_client import DEFAULT_CREDENTIALS
from fractal_client import FractalClient
from passlib.context import CryptContext
import os

from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import JobCreateV2
from fractal_server.app.schemas.v2 import ProjectCreateV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from fractal_server.app.security import UserOAuth


def create_first_user() -> None:
    context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    hashed_password = context.hash(DEFAULT_CREDENTIALS["password"])

    user_db = UserOAuth(
        email=DEFAULT_CREDENTIALS["username"],
        hashed_password=hashed_password,
        username="admin",
        slurm_user="slurm",
        is_superuser=True,
        is_verified=True,
    )

    with next(get_sync_db()) as session:
        session.add(user_db)
        session.commit()
        print("Admin created!")


if __name__ == "__main__":
    create_first_user()
    admin_client = FractalClient()

    # Task collection
    wheel_path = (
        Path(__file__).parents[1]
        / "tests/v2"
        / "fractal_tasks_mock"
        / "dist/fractal_tasks_mock-0.0.1-py3-none-any.whl"
    ).as_posix()
    admin_client.make_request(
        endpoint="api/v2/task/collect/pip/",
        method="POST",
        data=dict(package=wheel_path),
    )
    res = admin_client.make_request(
        endpoint="api/v2/task/",
        method="GET",
        data=dict(package=wheel_path),
    )
    task_list = res.json()
    task_name_to_id = {task["name"]: task["id"] for task in task_list}
    create_ome_zarr_compound_task_id = task_name_to_id[
        "create_ome_zarr_compound"
    ]

    # Find non-existing zarr_dir
    ind = 0
    while True:
        zarr_dir = f"/tmp/zarr_dir_{ind}/"  # nosec
        if not os.path.exists(zarr_dir):
            break
        ind += 1

    # Create DB resources
    proj = admin_client.add_project(ProjectCreateV2(name="MyProject"))
    ds = admin_client.add_dataset(
        proj.id, DatasetCreateV2(name="MyDataset", zarr_dir=zarr_dir)
    )
    wf = admin_client.add_workflow(
        proj.id, WorkflowCreateV2(name="MyWorkflow")
    )
    admin_client.add_workflowtask(
        proj.id,
        wf.id,
        create_ome_zarr_compound_task_id,
        WorkflowTaskCreateV2(args_non_parallel=dict(image_dir="/somewhere")),
    )

    # Submit job
    job = admin_client.submit_job(
        proj.id, wf.id, ds.id, applyworkflow=JobCreateV2()
    )
    admin_client.wait_for_all_jobs()
