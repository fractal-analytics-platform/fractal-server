import logging
import time
from json import JSONDecodeError
from typing import Any

import httpx
from a2wsgi import ASGIMiddleware
from fastapi import Response

from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user import UserRead
from fractal_server.app.schemas.user import UserUpdate
from fractal_server.app.schemas.v2 import DatasetCreateV2
from fractal_server.app.schemas.v2 import DatasetImportV2
from fractal_server.app.schemas.v2 import DatasetReadV2
from fractal_server.app.schemas.v2 import JobCreateV2
from fractal_server.app.schemas.v2 import JobReadV2
from fractal_server.app.schemas.v2 import ProjectCreateV2
from fractal_server.app.schemas.v2 import ProjectReadV2
from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.app.schemas.v2 import TaskReadV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowReadV2
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskReadV2
from fractal_server.main import app

DEFAULT_CREDENTIALS = {}
DEFAULT_CREDENTIALS["username"] = "admin@fractal.xy"
DEFAULT_CREDENTIALS["password"] = "1234"  # nosec


wsgi_app = ASGIMiddleware(app)
wsgi_app.app.state.jobsV1 = []
wsgi_app.app.state.jobsV2 = []
wsgi_app.app.state.fractal_ssh_list = None


def response_json(res: Response) -> dict[str, Any]:
    try:
        return res.json()
    except JSONDecodeError as e:
        raise ValueError(
            f"Error while parsing JSON body of response {res}.\n"
            f"Original error:\n{str(e)}\n"
            f"Reponse text\n:{res.text}"
        )


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
            try:
                print(response.json())
                self.bearer_token = response.json().get("access_token")
            except JSONDecodeError as e:
                logging.error("Could not parse response JSON.")
                logging.error(f"Request body: {credentials}")
                logging.error(f"API response: {response}")
                raise e

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

    def detail(self, res: httpx.Response):
        res_json = response_json(res)
        if res_json.get("detail"):
            raise ValueError(f"WARNING: {res_json.get('detail')}")

    def add_user(self, user: UserCreate):
        # Register new user
        res = self.make_request(
            endpoint="auth/register/",
            method="POST",
            data=user.dict(exclude_none=True),
        )
        self.detail(res)
        new_user_id = response_json(res)["id"]
        # Make new user verified
        patch_user = UserUpdate(is_verified=True)
        res = self.make_request(
            endpoint=f"auth/users/{new_user_id}/",
            method="PATCH",
            data=patch_user.dict(exclude_none=True),
        )
        self.detail(res)

        return UserRead(**response_json(res))

    def add_project(self, project: ProjectCreateV2):
        res = self.make_request(
            endpoint="api/v2/project/",
            method="POST",
            data=project.dict(),
        )
        self.detail(res)
        return ProjectReadV2(**response_json(res))

    def add_dataset(self, project_id, dataset: DatasetCreateV2):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/dataset/",
            method="POST",
            data=dataset.dict(),
        )
        self.detail(res)
        return DatasetReadV2(**response_json(res))

    def import_dataset(self, project_id, dataset: DatasetImportV2):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/dataset/import/",
            method="POST",
            data=dataset.dict(),
        )
        self.detail(res)
        return DatasetReadV2(**response_json(res))

    def add_workflow(self, project_id, workflow: WorkflowCreateV2):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/workflow/",
            method="POST",
            data=workflow.dict(),
        )
        self.detail(res)

        return WorkflowReadV2(**response_json(res))

    def add_workflowtask(
        self,
        project_id: int,
        workflow_id: int,
        task_id: int,
        wftask: WorkflowTaskCreateV2,
    ):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/workflow/"
            f"{workflow_id}/wftask/?{task_id=}",
            method="POST",
            data=wftask.dict(exclude_none=True),
        )
        self.detail(res)

        return WorkflowTaskReadV2(**response_json(res))

    def add_working_task(self):
        task = TaskCreateV2(
            name="Echo Task",
            command_non_parallel="echo",
            command_parallel="echo",
        )
        res = self.make_request(
            endpoint="api/v2/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res)
        return TaskReadV2(**response_json(res))

    def add_failing_task(self):
        task = TaskCreateV2(
            name="Ls Task",
            command_non_parallel="ls",
        )
        res = self.make_request(
            endpoint="api/v2/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res)
        return TaskReadV2(**response_json(res))

    def add_task(self, task: TaskCreateV2):
        res = self.make_request(
            endpoint="api/v2/task/",
            method="POST",
            data=task.dict(exclude_none=True),
        )
        self.detail(res)
        return TaskReadV2(**response_json(res))

    def whoami(self):
        res = self.make_request(
            endpoint="auth/current-user/",
            method="GET",
        )
        self.detail(res)
        return UserRead(**response_json(res))

    def patch_current_superuser(self, user: UserUpdate):
        me = self.whoami()
        res = self.make_request(
            endpoint=f"auth/users/{me.id}/",
            method="PATCH",
            data=user.dict(exclude_none=True),
        )
        self.detail(res)
        return UserRead(**response_json(res))

    def submit_job(
        self,
        project_id: int,
        workflow_id: int,
        dataset_id: int,
        applyworkflow: JobCreateV2,
    ):
        res = self.make_request(
            endpoint=(
                f"api/v2/project/{project_id}/job/submit/"
                f"?dataset_id={dataset_id}&workflow_id={workflow_id}"
            ),
            method="POST",
            data=applyworkflow.dict(exclude_none=True),
        )
        self.detail(res)

        return JobReadV2(**response_json(res))

    def wait_for_all_jobs(
        self,
        max_calls: int = 20,
        waiting_interval: float = 1.0,
    ):
        # Check if user is superuser or not, to set appropriate endpoint
        me = self.whoami()
        is_superuser = me.is_superuser
        if is_superuser:
            endpoint = "admin/v2/job/"
        else:
            endpoint = "api/v2/job/"
        # Make repeated calls
        for _ in range(max_calls):
            res = self.make_request(
                endpoint=endpoint,
                method="GET",
            )
            if res.status_code != 200:
                raise ValueError(f"Original error: {response_json(res)}")
            job_statuses = [job["status"] for job in response_json(res)]
            if "submitted" not in job_statuses:
                print("No submitted job left.")
                return None
            time.sleep(waiting_interval)
        raise RuntimeError(f"Reached {max_calls=} but {job_statuses=}.")
