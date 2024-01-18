from devtools import debug
from populate_clients import BASE_URL
from populate_clients import CREDENTIALS
from populate_clients import New
from populate_clients import SimpleHttpClient

from fractal_server.app.schemas import ApplyWorkflowCreate
from fractal_server.app.schemas import DatasetCreate
from fractal_server.app.schemas import ProjectCreate
from fractal_server.app.schemas import ResourceCreate
from fractal_server.app.schemas import TaskCreate
from fractal_server.app.schemas import WorkflowCreate
from fractal_server.app.schemas import WorkflowTaskCreate


if __name__ == "__main__":
    client = SimpleHttpClient(base_url=BASE_URL, credentials=CREDENTIALS)

    x = New(client=client)
    p = x.add_project(ProjectCreate(name="test"))
    debug(p)
    d = x.add_dataset(p.id, DatasetCreate(name="test_ds", type="zarr"))
    debug(d)
    r = x.add_resource(
        p.id, d.id, resource=ResourceCreate(path="/tmp")  # nosec
    )
    debug(r)
    w = x.add_workflow(p.id, WorkflowCreate(name="test_wf"))
    debug(w)
    t = x.add_task(
        TaskCreate(
            source="/",
            name="test_t",
            command="ls",
            input_type="zarr",
            output_type="zarr",
        )
    )
    debug(t)
    wt = x.add_workflowtask(p.id, w.id, t.id, WorkflowTaskCreate(order=0))
    debug(wt)
    a = x.add_job(p.id, w.id, d.id, d.id, applyworkflow=ApplyWorkflowCreate())
    debug(a)
