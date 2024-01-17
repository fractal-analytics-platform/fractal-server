from devtools import debug
from populate_clients import auth_client
from populate_clients import BASE_URL
from populate_clients import BearerHttpClient
from populate_clients import CREDENTIALS

from fractal_server.app.schemas import DatasetCreate
from fractal_server.app.schemas import DatasetRead
from fractal_server.app.schemas import ProjectCreate
from fractal_server.app.schemas import ProjectRead
from fractal_server.app.schemas import TaskCreate
from fractal_server.app.schemas import TaskRead
from fractal_server.app.schemas import WorkflowCreate
from fractal_server.app.schemas import WorkflowRead


class New:
    def __init__(self, bearer_client: BearerHttpClient):
        self.bearer_client = bearer_client

    def add_project(self, project_name):
        res = self.bearer_client.make_request(
            endpoint="api/v1/project/",
            method="POST",
            data=ProjectCreate(name=project_name).dict(),
        )

        return ProjectRead(**res.json())

    def add_dataset(self, project_id, dataset_name, type):
        res = self.bearer_client.make_request(
            endpoint=f"api/v1/project/{project_id}/dataset/",
            method="POST",
            data=DatasetCreate(name=dataset_name, type=type).dict(),
        )

        return DatasetRead(**res.json())

    def add_workflow(self, project_id, workflow_name):
        res = self.bearer_client.make_request(
            endpoint=f"api/v1/project/{project_id}/workflow/",
            method="POST",
            data=WorkflowCreate(name=workflow_name).dict(),
        )

        return WorkflowRead(**res.json())

    def add_task(
        self,
        source: str,
        name: str,
        command: str,
        input_type: str,
        output_type: str,
    ):

        res = self.bearer_client.make_request(
            endpoint="api/v1/task/",
            method="POST",
            data=TaskCreate(
                source=source,
                name=name,
                command=command,
                input_type=input_type,
                output_type=output_type,
            ).dict(
                exclude={
                    "meta",
                    "args_schema",
                    "docs_info",
                    "docs_link",
                    "version",
                    "args_schema_version",
                }
            ),
        )

        return TaskRead(**res.json())


if __name__ == "__main__":
    bearer_client = auth_client(base_url=BASE_URL, credentials=CREDENTIALS)

    x = New(bearer_client=bearer_client)
    p = x.add_project("test_p")
    debug(p)
    d = x.add_dataset(p.id, dataset_name="test_d", type="zarr")
    debug(d)
    w = x.add_workflow(p.id, workflow_name="test_w")
    debug(w)
    t = x.add_task(
        source="..",
        name="test_t",
        command="ls",
        input_type="zarr",
        output_type="zarr",
    )
    debug(t)
