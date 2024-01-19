from devtools import debug
from populate_clients import FractalClient
from populate_clients import SimpleHttpClient

from fractal_server.app.schemas import ApplyWorkflowCreate
from fractal_server.app.schemas import DatasetCreate
from fractal_server.app.schemas import ProjectCreate
from fractal_server.app.schemas import ResourceCreate
from fractal_server.app.schemas import UserCreate
from fractal_server.app.schemas import WorkflowCreate
from fractal_server.app.schemas import WorkflowTaskCreate


if __name__ == "__main__":
    base_client = SimpleHttpClient()
    admin = FractalClient(client=base_client)

    working_task = admin.add_working_task()
    failing_task = admin.add_failing_task()

    for ind_user in range(2):
        email = f"user{ind_user}@example.org"
        password = f"user{ind_user}-pwd"
        slurm_user = f"user{ind_user}-slurm"
        user_response = admin.add_user(
            UserCreate(
                email=email,
                password=password,
                slurm_user=slurm_user,
            )
        )
        debug(user_response)
        user_client = SimpleHttpClient(
            credentials=dict(username=email, password=password)
        )
        user = FractalClient(client=user_client)
        print(user.whoami())

        p = user.add_project(ProjectCreate(name="test"))
        d = user.add_dataset(p.id, DatasetCreate(name="test_ds", type="zarr"))
        r = user.add_resource(
            p.id, d.id, resource=ResourceCreate(path="/tmp")  # nosec
        )
        w = user.add_workflow(p.id, WorkflowCreate(name="test_wf"))
        wt = user.add_workflowtask(
            p.id, w.id, working_task.id, WorkflowTaskCreate()
        )
        a = user.apply_workflow(
            p.id, w.id, d.id, d.id, applyworkflow=ApplyWorkflowCreate()
        )

    admin.wait_for_all_jobs()
