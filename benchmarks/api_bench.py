import re

from locust import between
from locust import HttpUser
from locust import task


class FastAPIUser(HttpUser):

    wait_time = between(1, 3)  # time between requests in seconds

    def on_start(self):
        # this method is called when a Locust user starts executing.

        # perform login and save the authentication token
        login_response = self.client.post(
            "/auth/token/login/",
            data={"username": "admin@fractal.xy", "password": "1234"},
        )

        print(login_response.json())
        if login_response.status_code == 200:
            self.auth_token = login_response.json().get("access_token")
        else:
            print(
                f"Login failed: \n{login_response.status_code}\n"
                f"{login_response.text}"
            )
            self.environment.runner.quit()

    @task
    def test_fastapi_endpoints(self):
        swagger_url = "http://127.0.0.1:8000/openapi.json"

        response = self.client.get(swagger_url)

        if response.status_code == 200:
            swagger_data = response.json()

            paths = [
                path
                for path, path_data in swagger_data.get("paths", {}).items()
                if "get" in path_data
            ]

            # print(paths)
            patterns = [
                re.compile(r"/api/v1/task/"),
                re.compile(r"/auth/users/"),
                re.compile(r"/admin/"),
                re.compile(r"/status/"),
                re.compile(r"/download/"),
                re.compile(r"/export/"),
                re.compile(r"/import/"),
                re.compile(r"/export_history/"),
                re.compile(r"/workflow/"),
                re.compile(r"/job/"),
            ]

            cleaned_paths = [
                path
                for path in paths
                if not any(pattern.search(path) for pattern in patterns)
            ]
            print(cleaned_paths)

            headers = {"Authorization": f"Bearer {self.auth_token}"}

            pattern = re.compile(r"\{.*?\}")
            # /api/v1/project/{project_id} -> /api/v1/project/X

            # Iterate through each path and test the corresponding endpoint
            for i in range(1, 10):
                paths_with_param = [
                    re.sub(pattern, str(i), path) for path in cleaned_paths
                ]
                for path in paths_with_param:
                    response = self.client.get(path, headers=headers)
