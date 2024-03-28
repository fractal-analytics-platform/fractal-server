import logging
import time

import httpx
from a2wsgi import ASGIMiddleware

from fractal_server.app.schemas.v1 import ApplyWorkflowCreateV1
from fractal_server.app.schemas.v1 import ApplyWorkflowReadV1
from fractal_server.app.schemas.v1 import DatasetCreateV1
from fractal_server.app.schemas.v1 import DatasetReadV1
from fractal_server.app.schemas.v1 import ProjectCreateV1
from fractal_server.app.schemas.v1 import ProjectReadV1
from fractal_server.app.schemas.v1 import ResourceCreateV1
from fractal_server.app.schemas.v1 import ResourceReadV1
from fractal_server.app.schemas.v1 import TaskCreateV1
from fractal_server.app.schemas.v1 import TaskReadV1
from fractal_server.app.schemas.v1 import UserCreate
from fractal_server.app.schemas.v1 import UserRead
from fractal_server.app.schemas.v1 import UserUpdate
from fractal_server.app.schemas.v1 import WorkflowCreateV1
from fractal_server.app.schemas.v1 import WorkflowReadV1
from fractal_server.app.schemas.v1 import WorkflowTaskCreateV1
from fractal_server.app.schemas.v1 import WorkflowTaskReadV1
from fractal_server.main import app

DEFAULT_CREDENTIALS = {}
DEFAULT_CREDENTIALS["username"] = "admin@fractal.xy"
DEFAULT_CREDENTIALS["password"] = "1234"  # nosec


wsgi_app = ASGIMiddleware(app)


class FractalClient:
    def __init__(
        self,
        credentials: dict[str, str] = DEFAULT_CREDENTIALS,
    ):
        # base_url is needed to determine the communication protocol
        # for httpx, that uses requests, otherwise a KeyError is raised.
        with httpx.Client(app=wsgi_app, base_url="http://") as client:
            response = client.post(
                "/auth/token/login/",
                data=credentials,
            )
            print(response.json())
            self.bearer_token = response.json().get("access_token")

    def make_request(self, endpoint, method="GET", data=None):
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        url = f"/{endpoint}"

        try:
            with httpx.Client(app=wsgi_app, base_url="http://") as client:
                time_start = time.perf_counter()
                if method == "GET":
                    response = client.get(url, headers=headers)
                elif method == "POST":
                    response = client.post(url, json=data, headers=headers)
                elif method == "PATCH":
                    response = client.patch(url, json=data, headers=headers)
                elif method == "DELETE":
                    response = client.delete(url, headers=headers)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

            # Log calls that take more than one second
            time_end = time.perf_counter()
            elapsed = time_end - time_start
            if elapsed > 1:
                logging.warning(
                    f"SLOW API CALL: {method} {url}, {elapsed} seconds"
                )

            return response

        except httpx.ReadError as e:
            print(f"Request failed: {e}")
            return None

    def detail(self, res):
        if res.get("detail"):
            raise ValueError(f"Attention: {res.get('detail')}")

    def add_user(self, user: UserCreate):
        # Register new user
        res = self.make_request(
            endpoint="auth/register/",
            method="POST",
            data=user.dict(exclude_none=True),
        )
        self.detail(res.json())
        new_user_id = res.json()["id"]
        # Make new user verified
        patch_user = UserUpdate(is_verified=True)
        res = self.make_request(
            endpoint=f"auth/users/{new_user_id}/",
            method="PATCH",
            data=patch_user.dict(exclude_none=True),
        )
        self.detail(res.json())

        return UserRead(**res.json())

    def add_project(self, project: ProjectCreateV1):
        res = self.make_request(
            endpoint="api/v1/project/",
            method="POST",
            data=project.dict(),
        )
        self.detail(res.json())
        return ProjectReadV1(**res.json())

    def add_dataset(self, project_id, dataset: DatasetCreateV1):
        res = self.make_request(
            endpoint=f"api/v1/project/{project_id}/dataset/",
            method="POST",
            data=dataset.dict(),
        )
        self.detail(res.json())
        return DatasetReadV1(**res.json())

    def add_resource(
        self, project_id: int, dataset_id: int, resource: ResourceCreateV1
    ):
        res = self.make_request(
            endpoint=f"api/v1/project/{project_id}/dataset/"
            f"{dataset_id}/resource/",
            method="POST",
            data=resource.dict(),
        )
        self.detail(res.json())

        return ResourceReadV1(**res.json())

    def add_workflow(self, project_id, workflow: WorkflowCreateV1):
        res = self.make_request(
            endpoint=f"api/v1/project/{project_id}/workflow/",
            method="POST",
            data=workflow.dict(),
        )
        self.detail(res.json())

        return WorkflowReadV1(**res.json())

    def add_workflowtask(
        self,
        project_id: int,
        workflow_id: int,
        task_id: int,
        wftask: WorkflowTaskCreateV1,
    ):
        res = self.make_request(
            endpoint=f"api/v1/project/{project_id}/workflow/"
            f"{workflow_id}/wftask/?{task_id=}",
            method="POST",
            data=wftask.dict(exclude_none=True),
        )
        self.detail(res.json())

        return WorkflowTaskReadV1(**res.json())

    def add_working_task(self):
        task = TaskCreateV1(
            source="echo-task",
            name="Echo Task",
            command="echo",
            input_type="Any",
            output_type="Any",
        )
        res = self.make_request(
            endpoint="api/v1/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res.json())
        return TaskReadV1(**res.json())

    def add_failing_task(self):
        task = TaskCreateV1(
            source="ls-task",
            name="Ls Task",
            command="ls",
            input_type="Any",
            output_type="Any",
        )
        res = self.make_request(
            endpoint="api/v1/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res.json())
        return TaskReadV1(**res.json())

    def add_task(self, task: TaskCreateV1):
        res = self.make_request(
            endpoint="api/v1/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res.json())
        return TaskReadV1(**res.json())

    def whoami(self):
        res = self.make_request(
            endpoint="auth/current-user/",
            method="GET",
        )
        self.detail(res.json())
        return UserRead(**res.json())

    def patch_current_superuser(self, user: UserUpdate):
        me = self.whoami()
        res = self.make_request(
            endpoint=f"auth/users/{me.id}/",
            method="PATCH",
            data=user.dict(exclude_none=True),
        )
        self.detail(res.json())
        return UserRead(**res.json())

    def apply_workflow(
        self,
        project_id: int,
        workflow_id: int,
        in_dataset_id: int,
        out_dataset_id: int,
        applyworkflow: ApplyWorkflowCreateV1,
    ):
        res = self.make_request(
            endpoint=f"api/v1/project/{project_id}/"
            f"workflow/{workflow_id}/apply/"
            f"?input_dataset_id={in_dataset_id}"
            f"&output_dataset_id={out_dataset_id}",
            method="POST",
            data=applyworkflow.dict(exclude_none=True),
        )
        self.detail(res.json())

        return ApplyWorkflowReadV1(**res.json())

    def wait_for_all_jobs(
        self,
        max_calls: int = 20,
        waiting_interval: float = 1.0,
    ):
        # Check if user is superuser or not, to set appropriate endpoint
        me = self.whoami()
        is_superuser = me.is_superuser
        if is_superuser:
            endpoint = "admin/job/"
        else:
            endpoint = "api/v1/job/"
        # Make repeated calls
        for ind_call in range(max_calls):
            res = self.make_request(
                endpoint=endpoint,
                method="GET",
            )
            if res.status_code != 200:
                raise
            job_statuses = [job["status"] for job in res.json()]
            if "submitted" not in job_statuses:
                print("No submitted job left.")
                return None
            time.sleep(waiting_interval)
        raise RuntimeError(f"Reached {max_calls=} but {job_statuses=}.")
