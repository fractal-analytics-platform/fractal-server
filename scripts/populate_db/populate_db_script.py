from fractal_client import DEFAULT_CREDENTIALS
from fractal_client import FractalClient
from passlib.context import CryptContext

from fractal_server.app.db import get_sync_db
from fractal_server.app.schemas import ApplyWorkflowCreate
from fractal_server.app.schemas import DatasetCreate
from fractal_server.app.schemas import ProjectCreate
from fractal_server.app.schemas import ResourceCreate
from fractal_server.app.schemas import UserCreate
from fractal_server.app.schemas import WorkflowCreate
from fractal_server.app.schemas import WorkflowTaskCreate
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


def _user_flow_vanilla(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="vanilla")
    proj = user.add_project(ProjectCreate(name="MyProject"))
    ds = user.add_dataset(
        proj.id, DatasetCreate(name="MyDataset", type="type")
    )
    user.add_resource(
        proj.id,
        ds.id,
        resource=ResourceCreate(path="/invalidpath"),
    )
    wf = user.add_workflow(proj.id, WorkflowCreate(name="MyWorkflow"))
    user.add_workflowtask(
        proj.id, wf.id, working_task_id, WorkflowTaskCreate()
    )
    user.apply_workflow(
        proj.id, wf.id, ds.id, ds.id, applyworkflow=ApplyWorkflowCreate()
    )


def _user_flow_power(
    admin: FractalClient,
    *,
    working_task_id: int,
    failing_task_id: int,
):
    user = _create_user_client(admin, user_identifier="power")
    proj = user.add_project(ProjectCreate(name="MyProject"))

    num_workflows = 10
    num_jobs_per_workflow = 20
    for ind_wf in range(num_workflows):
        wf = user.add_workflow(
            proj.id, WorkflowCreate(name=f"MyWorkflow-{ind_wf}")
        )
        user.add_workflowtask(
            proj.id, wf.id, working_task_id, WorkflowTaskCreate()
        )
        if ind_wf % 2 == 0:
            user.add_workflowtask(
                proj.id, wf.id, failing_task_id, WorkflowTaskCreate()
            )
        for ind_job in range(num_jobs_per_workflow):
            ds = user.add_dataset(
                proj.id, DatasetCreate(name="MyDataset", type="type")
            )
            user.add_resource(
                proj.id,
                ds.id,
                resource=ResourceCreate(path="/invalidpath"),
            )
            user.apply_workflow(
                proj.id,
                wf.id,
                ds.id,
                ds.id,
                applyworkflow=ApplyWorkflowCreate(),
            )


if __name__ == "__main__":
    create_first_user()
    admin = FractalClient()

    working_task = admin.add_working_task()
    failing_task = admin.add_failing_task()

    _user_flow_vanilla(admin, working_task_id=working_task.id)
    _user_flow_power(
        admin, working_task_id=working_task.id, failing_task_id=failing_task.id
    )

    admin.wait_for_all_jobs()
