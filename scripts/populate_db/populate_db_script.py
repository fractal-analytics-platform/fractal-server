from fractal_client import DEFAULT_CREDENTIALS
from fractal_client import FractalClient
from passlib.context import CryptContext

from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas import UserCreate
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


def _create_user_client(
    _admin: FractalClient, user_identifier: str
) -> FractalClient:
    email = f"{user_identifier}@example.org"
    password = f"{user_identifier}-pwd"
    slurm_user = f"{user_identifier}-slurm"
    _admin.add_user(
        UserCreate(
            email=email,
            password=password,
            slurm_user=slurm_user,
        )
    )
    _user = FractalClient(credentials=dict(username=email, password=password))
    print(_user.whoami())
    return _user


# vanilla user:
# 1 project
# 1 dataset with a single resource
# 1 workflow with a single task
# 1 job
def _user_flow_vanilla(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="vanilla")
    proj = user.add_project(ProjectCreateV2(name="MyProject_uv"))
    ds = user.add_dataset(
        proj.id, DatasetCreateV2(name="MyDataset", zarr_dir="/tmp/zarr")
    )
    wf = user.add_workflow(proj.id, WorkflowCreateV2(name="MyWorkflow"))
    user.add_workflowtask(
        proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
    )
    user.submit_job(proj.id, wf.id, ds.id, applyworkflow=JobCreateV2())


# power user:
# 1 project
# `num_jobs_per_workflow` datasets with a single resource
# `num_workflows` workflows, half with 1, half with 3 tasks
# `num_jobs_per_workflow` jobs
def _user_flow_power(
    admin: FractalClient,
    *,
    working_task_id: int,
    failing_task_id: int,
):
    user = _create_user_client(admin, user_identifier="power")
    proj = user.add_project(ProjectCreateV2(name="MyProject_upw"))

    num_workflows = 20
    num_jobs_per_workflow = 20
    for ind_wf in range(num_workflows):
        wf = user.add_workflow(
            proj.id, WorkflowCreateV2(name=f"MyWorkflow-{ind_wf}")
        )
        user.add_workflowtask(
            proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
        )
        if ind_wf % 2 == 0:
            user.add_workflowtask(
                proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
            )
            user.add_workflowtask(
                proj.id, wf.id, failing_task_id, WorkflowTaskCreateV2()
            )
        for ind_job in range(num_jobs_per_workflow):
            ds = user.add_dataset(
                proj.id,
                DatasetCreateV2(name="MyDataset", zarr_dir="/tmp/zarr"),
            )
            user.submit_job(
                proj.id,
                wf.id,
                ds.id,
                applyworkflow=JobCreateV2(),
            )


# dataset user:
# 1 project
# `n_datasets` datasets with a single resource
# `num_workflows` workflows, with a single task
# `n_datasets` jobs
def _user_flow_dataset(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="dataset")
    proj = user.add_project(ProjectCreateV2(name="MyProject_us"))
    n_datasets = 20
    ds_list = []
    for i in range(n_datasets):
        ds = user.add_dataset(
            proj.id,
            DatasetCreateV2(name=f"MyDataset_us-{i}", zarr_dir="/tmp/zarr"),
        )
        ds_list.append(ds)

    num_workflows = 20
    for i in range(num_workflows):
        wf = user.add_workflow(
            proj.id, WorkflowCreateV2(name=f"MyWorkflow_us-{i}")
        )
        user.add_workflowtask(
            proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
        )
        for ds in ds_list:
            user.submit_job(
                proj.id,
                wf.id,
                ds.id,
                applyworkflow=JobCreateV2(),
            )


# project user:
# `n_projects` project
# 2 datasets with a single resource per project
# 1 workflow, with a single task
# `num_jobs_per_workflow` jobs
def _user_flow_project(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="project")
    n_projects = 25
    num_jobs_per_workflow = 5
    for i in range(n_projects):
        proj = user.add_project(ProjectCreateV2(name=f"MyProject_upj-{i}"))
        ds = user.add_dataset(
            proj.id,
            DatasetCreateV2(name=f"MyDataset_up-{i}", zarr_dir="/tmp/zarr"),
        )
        wf = user.add_workflow(
            proj.id, WorkflowCreateV2(name=f"MyWorkflow_up-{i}")
        )
        user.add_workflowtask(
            proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
        )
        for i in range(num_jobs_per_workflow):
            user.submit_job(
                proj.id,
                wf.id,
                ds.id,
                applyworkflow=JobCreateV2(),
            )


# job user:
# 1 project
# 1 dataset with a single resource
# 1 workflow with a single task
# `num_jobs_per_workflow` job
def _user_flow_job(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="job")
    proj = user.add_project(ProjectCreateV2(name="MyProject_uj"))
    ds = user.add_dataset(
        proj.id, DatasetCreateV2(name="MyDataset_uj", type="type")
    )
    wf = user.add_workflow(proj.id, WorkflowCreateV2(name="MyWorkflow_uj"))
    user.add_workflowtask(
        proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
    )
    num_jobs_per_workflow = 100
    for i in range(num_jobs_per_workflow):
        user.submit_job(proj.id, wf.id, ds.id, applyworkflow=JobCreateV2())


if __name__ == "__main__":
    create_first_user()
    admin = FractalClient()

    working_task = admin.add_working_task()
    failing_task = admin.add_failing_task()

    _user_flow_vanilla(admin, working_task_id=working_task.id)
    _user_flow_power(
        admin, working_task_id=working_task.id, failing_task_id=failing_task.id
    )
    _user_flow_dataset(admin, working_task_id=working_task.id)
    _user_flow_project(admin, working_task_id=working_task.id)
    _user_flow_job(admin, working_task_id=working_task.id)

    admin.wait_for_all_jobs()
