import logging
import time
from json import JSONDecodeError
from typing import Any

import httpx
from a2wsgi import ASGIMiddleware
from fastapi import Response

from fractal_server.app.schemas import UserGroupCreate
from fractal_server.app.schemas import UserGroupRead
from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.user import UserRead
from fractal_server.app.schemas.user import UserUpdate
from fractal_server.app.schemas.v2 import DatasetCreate
from fractal_server.app.schemas.v2 import DatasetImport
from fractal_server.app.schemas.v2 import DatasetRead
from fractal_server.app.schemas.v2 import JobCreate
from fractal_server.app.schemas.v2 import JobRead
from fractal_server.app.schemas.v2 import ProjectCreate
from fractal_server.app.schemas.v2 import ProjectRead
from fractal_server.app.schemas.v2 import TaskCreate
from fractal_server.app.schemas.v2 import TaskRead
from fractal_server.app.schemas.v2 import WorkflowCreate
from fractal_server.app.schemas.v2 import WorkflowRead
from fractal_server.app.schemas.v2 import WorkflowTaskCreate
from fractal_server.app.schemas.v2 import WorkflowTaskRead
from fractal_server.main import app

DEFAULT_CREDENTIALS = {}
DEFAULT_CREDENTIALS["username"] = "admin@example.org"
DEFAULT_CREDENTIALS["password"] = "1234"  # nosec


wsgi_app = ASGIMiddleware(app)
wsgi_app.app.state.jobs = []
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
        with httpx.Client(
            base_url="http://", transport=httpx.WSGITransport(app=wsgi_app)
        ) as client:
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
            with httpx.Client(
                base_url="http://", transport=httpx.WSGITransport(app=wsgi_app)
            ) as client:
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
            data=user.model_dump(exclude_none=True),
        )
        self.detail(res)
        new_user_id = response_json(res)["id"]
        # Make new user verified
        patch_user = UserUpdate(is_verified=True)
        res = self.make_request(
            endpoint=f"auth/users/{new_user_id}/",
            method="PATCH",
            data=patch_user.model_dump(exclude_none=True),
        )
        self.detail(res)

        return UserRead(**response_json(res))

    def associate_user_with_profile(self, user_id: int):
        res = self.make_request(
            endpoint="admin/v2/resource/",
            method="GET",
        )
        resources = res.json()
        if not resources:
            raise ValueError(f"Found {resources=}")
        resource = resources[0]
        res = self.make_request(
            endpoint=f"admin/v2/resource/{resource['id']}/profile/",
            method="GET",
        )
        profiles = res.json()
        if not profiles:
            raise ValueError(f"Found {profiles=}")
        profile = profiles[0]
        res = self.make_request(
            endpoint=f"auth/users/{user_id}/",
            method="PATCH",
            data=dict(profile_id=profile["id"]),
        )
        self.detail(res)

    def add_project(self, project: ProjectCreate):
        res = self.make_request(
            endpoint="api/v2/project/",
            method="POST",
            data=project.model_dump(),
        )
        self.detail(res)
        return ProjectRead(**response_json(res))

    def add_dataset(self, project_id, dataset: DatasetCreate):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/dataset/",
            method="POST",
            data=dataset.model_dump(),
        )
        self.detail(res)
        return DatasetRead(**response_json(res))

    def import_dataset(self, project_id, dataset: DatasetImport):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/dataset/import/",
            method="POST",
            data=dataset.model_dump(),
        )
        self.detail(res)
        return DatasetRead(**response_json(res))

    def add_workflow(self, project_id, workflow: WorkflowCreate):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/workflow/",
            method="POST",
            data=workflow.model_dump(),
        )
        self.detail(res)

        return WorkflowRead(**response_json(res))

    def add_workflowtask(
        self,
        project_id: int,
        workflow_id: int,
        task_id: int,
        wftask: WorkflowTaskCreate,
    ):
        res = self.make_request(
            endpoint=f"api/v2/project/{project_id}/workflow/"
            f"{workflow_id}/wftask/?{task_id=}",
            method="POST",
            data=wftask.model_dump(exclude_none=True),
        )
        self.detail(res)

        return WorkflowTaskRead(**response_json(res))

    def add_working_task(self):
        task = TaskCreate(
            name="Echo Task",
            command_non_parallel="echo",
            command_parallel="echo",
        )
        res = self.make_request(
            endpoint="api/v2/task/",
            method="POST",
            data=task.model_dump(exclude_none=True),
        )
        self.detail(res)
        return TaskRead(**response_json(res))

    def add_failing_task(self):
        task = TaskCreate(
            name="Ls Task",
            command_non_parallel="ls",
        )
        res = self.make_request(
            endpoint="api/v2/task/",
            method="POST",
            data=task.model_dump(exclude_none=True),
        )
        self.detail(res)
        return TaskRead(**response_json(res))

    def add_task(self, task: TaskCreate):
        res = self.make_request(
            endpoint="api/v2/task/",
            method="POST",
            data=task.model_dump(exclude_none=True),
        )
        self.detail(res)
        return TaskRead(**response_json(res))

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
            data=user.model_dump(exclude_none=True),
        )
        self.detail(res)
        return UserRead(**response_json(res))

    def submit_job(
        self,
        project_id: int,
        workflow_id: int,
        dataset_id: int,
        applyworkflow: JobCreate,
    ):
        res = self.make_request(
            endpoint=(
                f"api/v2/project/{project_id}/job/submit/"
                f"?dataset_id={dataset_id}&workflow_id={workflow_id}"
            ),
            method="POST",
            data=applyworkflow.model_dump(exclude_none=True),
        )
        self.detail(res)

        return JobRead(**response_json(res))

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
            use_pagination = True
        else:
            endpoint = "api/v2/job/"
            use_pagination = False
        # Make repeated calls
        for _ in range(max_calls):
            res = self.make_request(
                endpoint=endpoint,
                method="GET",
            )
            if res.status_code != 200:
                raise ValueError(f"Original error: {response_json(res)}")
            if use_pagination:
                actual_response_items = response_json(res)["items"]
            else:
                actual_response_items = response_json(res)
            job_statuses = [job["status"] for job in actual_response_items]
            if "submitted" not in job_statuses:
                print("No submitted job left.")
                return None
            time.sleep(waiting_interval)
        raise RuntimeError(f"Reached {max_calls=} but {job_statuses=}.")

    def add_user_group(self):
        group = UserGroupCreate(name="new_group")
        res = self.make_request(
            endpoint="auth/group/",
            method="POST",
            data=group.model_dump(),
        )
        self.detail(res)
        return UserGroupRead(**response_json(res))

    def add_user_usergroup(self, group_id: int, user_id: int):
        res = self.make_request(
            endpoint=f"auth/group/{group_id}/add-user/{user_id}/",
            method="POST",
        )
        self.detail(res)
        return UserGroupRead(**response_json(res))
