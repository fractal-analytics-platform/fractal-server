import time

import requests
from devtools import debug

from fractal_server.app.schemas import ApplyWorkflowCreate
from fractal_server.app.schemas import ApplyWorkflowRead
from fractal_server.app.schemas import DatasetCreate
from fractal_server.app.schemas import DatasetRead
from fractal_server.app.schemas import ProjectCreate
from fractal_server.app.schemas import ProjectRead
from fractal_server.app.schemas import ResourceCreate
from fractal_server.app.schemas import ResourceRead
from fractal_server.app.schemas import TaskCreate
from fractal_server.app.schemas import TaskRead
from fractal_server.app.schemas import UserCreate
from fractal_server.app.schemas import UserRead
from fractal_server.app.schemas import UserUpdate
from fractal_server.app.schemas import WorkflowCreate
from fractal_server.app.schemas import WorkflowRead
from fractal_server.app.schemas import WorkflowTaskCreate
from fractal_server.app.schemas import WorkflowTaskRead


DEFAULT_BASE_URL = "http://localhost:8000"
DEFAULT_CREDENTIALS = {}
DEFAULT_CREDENTIALS["username"] = "admin@fractal.xy"
DEFAULT_CREDENTIALS["password"] = "1234"  # nosec


class SimpleHttpClient:
    base_url: str

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        credentials: dict[str, str] = DEFAULT_CREDENTIALS,
    ):
        self.base_url = base_url
        response = requests.post(
            f"{self.base_url}/auth/token/login/",
            data=credentials,
        )
        self.bearer_token = response.json().get("access_token")

    def make_request(self, endpoint, method="GET", data=None):
        headers = {"Authorization": f"Bearer {self.bearer_token}"}
        url = f"{self.base_url}/{endpoint}"

        try:
            time_start = time.perf_counter()
            if method == "GET":
                response = requests.get(url, headers=headers)
            elif method == "POST":
                response = requests.post(url, json=data, headers=headers)
            elif method == "PATCH":
                response = requests.patch(url, json=data, headers=headers)
            elif method == "DELETE":
                response = requests.delete(url, headers=headers)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            time_end = time.perf_counter()
            elapsed = time_end - time_start
            if elapsed > 1:
                debug(url, method, elapsed)

            return response

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None


class FractalClient:
    def __init__(self, client: SimpleHttpClient):
        self.client = client

    def detail(self, res):
        if res.get("detail"):
            raise ValueError(f"Attention: {res.get('detail')}")

    def add_user(self, user: UserCreate):
        # Register new user
        res = self.client.make_request(
            endpoint="auth/register/",
            method="POST",
            data=user.dict(exclude_none=True),
        )
        self.detail(res.json())
        new_user_id = res.json()["id"]
        # Make new user verified
        patch_user = UserUpdate(is_verified=True)
        res = self.client.make_request(
            endpoint=f"auth/users/{new_user_id}/",
            method="PATCH",
            data=patch_user.dict(exclude_none=True),
        )
        self.detail(res.json())

        return UserRead(**res.json())

    def add_project(self, project: ProjectCreate):
        res = self.client.make_request(
            endpoint="api/v1/project/",
            method="POST",
            data=project.dict(),
        )
        self.detail(res.json())
        return ProjectRead(**res.json())

    def add_dataset(self, project_id, dataset: DatasetCreate):
        res = self.client.make_request(
            endpoint=f"api/v1/project/{project_id}/dataset/",
            method="POST",
            data=dataset.dict(),
        )
        self.detail(res.json())
        return DatasetRead(**res.json())

    def add_resource(
        self, project_id: int, dataset_id: int, resource: ResourceCreate
    ):
        res = self.client.make_request(
            endpoint=f"api/v1/project/{project_id}/dataset/"
            f"{dataset_id}/resource/",
            method="POST",
            data=resource.dict(),
        )
        self.detail(res.json())

        return ResourceRead(**res.json())

    def add_workflow(self, project_id, workflow: WorkflowCreate):
        res = self.client.make_request(
            endpoint=f"api/v1/project/{project_id}/workflow/",
            method="POST",
            data=workflow.dict(),
        )
        self.detail(res.json())

        return WorkflowRead(**res.json())

    def add_workflowtask(
        self,
        project_id: int,
        workflow_id: int,
        task_id: int,
        wftask: WorkflowTaskCreate,
    ):
        res = self.client.make_request(
            endpoint=f"api/v1/project/{project_id}/workflow/"
            f"{workflow_id}/wftask/?{task_id=}",
            method="POST",
            data=wftask.dict(exclude_none=True),
        )
        self.detail(res.json())

        return WorkflowTaskRead(**res.json())

    def add_working_task(self):
        task = TaskCreate(
            source="echo-task",
            name="Echo Task",
            command="echo",
            input_type="Any",
            output_type="Any",
        )
        res = self.client.make_request(
            endpoint="api/v1/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res.json())
        return TaskRead(**res.json())

    def add_failing_task(self):
        task = TaskCreate(
            source="ls-task",
            name="Ls Task",
            command="ls",
            input_type="Any",
            output_type="Any",
        )
        res = self.client.make_request(
            endpoint="api/v1/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res.json())
        return TaskRead(**res.json())

    def add_task(self, task: TaskCreate):
        res = self.client.make_request(
            endpoint="api/v1/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res.json())
        return TaskRead(**res.json())

    def whoami(self):
        res = self.client.make_request(
            endpoint="auth/current-user/",
            method="GET",
        )
        self.detail(res.json())
        return UserRead(**res.json())

    def patch_current_superuser(self, user: UserUpdate):
        me = self.whoami()
        res = self.client.make_request(
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
        applyworkflow: ApplyWorkflowCreate,
    ):
        res = self.client.make_request(
            endpoint=f"api/v1/project/{project_id}/"
            f"workflow/{workflow_id}/apply/"
            f"?input_dataset_id={in_dataset_id}"
            f"&output_dataset_id={out_dataset_id}",
            method="POST",
            data=applyworkflow.dict(exclude_none=True),
        )
        self.detail(res.json())

        return ApplyWorkflowRead(**res.json())

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
            res = self.client.make_request(
                endpoint=endpoint,
                method="GET",
            )
            if res.status_code != 200:
                raise
            job_statuses = [job["status"] for job in res.json()]
            print(job_statuses)
            if ("submitted" not in job_statuses) and (
                "running" not in job_statuses
            ):
                return None
            time.sleep(waiting_interval)
        raise RuntimeError(f"Reached {max_calls=} but {job_statuses=}.")
