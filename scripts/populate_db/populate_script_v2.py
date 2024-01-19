import time

from populate_clients import FractalClient
from populate_clients import SimpleHttpClient

from fractal_server.app.schemas import ApplyWorkflowCreate
from fractal_server.app.schemas import DatasetCreate
from fractal_server.app.schemas import ProjectCreate
from fractal_server.app.schemas import ResourceCreate
from fractal_server.app.schemas import UserCreate
from fractal_server.app.schemas import WorkflowCreate
from fractal_server.app.schemas import WorkflowTaskCreate


def _create_user_client(_admin: FractalClient, _identifier) -> FractalClient:
    email = f"user{_identifier}@example.org"
    password = f"user{_identifier}-pwd"
    slurm_user = f"user{_identifier}-slurm"
    _admin.add_user(
        UserCreate(
            email=email,
            password=password,
            slurm_user=slurm_user,
        )
    )
    user_client = SimpleHttpClient(
        credentials=dict(username=email, password=password)
    )
    _user = FractalClient(client=user_client)
    print(_user.whoami())
    return _user


if __name__ == "__main__":
    base_client = SimpleHttpClient()
    admin = FractalClient(client=base_client)

    working_task = admin.add_working_task()
    failing_task = admin.add_failing_task()

    num_users = 2
    num_projects = 2
    num_jobs = 10

    for ind_user in range(num_users):
        user = _create_user_client(admin, ind_user)
        for ind_p in range(num_projects):
            p = user.add_project(ProjectCreate(name=f"proj-{ind_p}"))
            for ind_job in range(num_jobs):
                d = user.add_dataset(
                    p.id, DatasetCreate(name=f"ds-{ind_job}", type="zarr")
                )
                r = user.add_resource(
                    p.id,
                    d.id,
                    resource=ResourceCreate(path=f"/invalid_{ind_job}"),
                )
                w = user.add_workflow(
                    p.id, WorkflowCreate(name=f"wf-{ind_job}")
                )
                user.add_workflowtask(
                    p.id, w.id, working_task.id, WorkflowTaskCreate()
                )
                a = user.apply_workflow(
                    p.id, w.id, d.id, d.id, applyworkflow=ApplyWorkflowCreate()
                )
                time.sleep(0.1)

    admin.wait_for_all_jobs()
