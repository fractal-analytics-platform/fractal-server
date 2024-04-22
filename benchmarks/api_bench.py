import json
import sys
import time
from datetime import datetime
from typing import Any
from typing import Optional

import httpx
from httpx import Client
from httpx import Response
from jinja2 import Environment
from jinja2 import FileSystemLoader
from pydantic import BaseModel


FRACTAL_SERVER_URL = "http://localhost:8000"


class UserBench(BaseModel):
    name: str
    password: str
    token: Optional[str]


USERS = [
    UserBench(name="vanilla@example.org", password="vanilla-pwd"),  # nosec
    UserBench(name="power@example.org", password="power-pwd"),  # nosec
    UserBench(name="dataset@example.org", password="dataset-pwd"),  # nosec
    UserBench(name="project@example.org", password="project-pwd"),  # nosec
    UserBench(name="job@example.org", password="job-pwd"),  # nosec
]


N_REQUESTS = 25

API_PATHS = [
    "/api/alive/",
    "/api/v2/dataset/",
    "/api/v2/job/",
    "/api/v2/project/",
    "/api/v2/workflow/",
]

# def get_clean_API_paths() -> list[str]:
#     """
#     Extract endpoint paths by filtering the OpenAPI ones.
#     """
#     swagger_url = f"{FRACTAL_SERVER_URL}/openapi.json"
#     response = httpx.get(swagger_url)
#     if response.status_code == 200:
#         swagger_data = response.json()
#         paths = [
#             path
#             for path, path_data in swagger_data.get("paths", {}).items()
#             if "get" in path_data
#         ]
#         excluded_patterns = [
#             re.compile(r"/api/v1/"),
#             re.compile(r"/api/v2/task/"),
#             re.compile(r"/api/v2/task-legacy/"),
#             re.compile(r"/auth/"),
#             re.compile(r"/admin/"),
#             re.compile(r"/status/"),
#             re.compile(r"/download/"),
#             re.compile(r"/export/"),
#             re.compile(r"/import/"),
#             re.compile(r"/export_history/"),
#             re.compile(r"\{.*?\}"),
#         ]
#         API_paths = [
#             path
#             for path in paths
#             if not any(pattern.search(path) for pattern in excluded_patterns)
#         ]
#     return API_paths


class Benchmark:
    def __init__(
        self,
        method: str,
        cleaned_paths: list[str],
        users: list[UserBench],
        current_branch: str,
    ):

        self.method = method
        self.cleaned_paths = cleaned_paths
        self.users = users
        self.client = Client(base_url=FRACTAL_SERVER_URL)
        self.current_branch = current_branch
        self.exceptions: list = []

        max_steps = 4
        for user in self.users:
            for step in range(max_steps):
                user.token = (
                    self.client.post(
                        "/auth/token/login/",
                        data=dict(username=user.name, password=user.password),
                    )
                    .json()
                    .get("access_token")
                )
                if user.token is not None:
                    break
                time.sleep(0.5)
            if user.token is None:
                sys.exit(f"Error while logging-in as user {user.name}")

        # print("Users:")
        # for _user in self.users:
        #     print(_user)

    def aggregate_on_path(
        self, user_metrics: list[dict[str, Any]]
    ) -> dict[str, list]:
        """
        Given a list of benchmarks, aggregate them on their "path" keys.
        """
        aggregated_values = {}
        for bench in user_metrics:
            current_path = bench.pop("path")
            if current_path not in aggregated_values.keys():
                aggregated_values[current_path] = []
            aggregated_values[current_path].append(bench)
        return aggregated_values

    def to_html(self, aggregated_values: dict, n_requests: int):

        env = Environment(
            loader=FileSystemLoader(searchpath="./templates"), autoescape=True
        )
        template = env.get_template("bench_template.html")

        rendered_html = template.render(
            user_metrics=aggregated_values,
            method=self.method,
            date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            requests=n_requests,
        )

        with open("bench.html", "w") as output_file:
            output_file.write(rendered_html)

    def make_md_diff(self, agg_values_main: dict, agg_values_curr: dict):

        env = Environment(
            loader=FileSystemLoader(searchpath="./templates"), autoescape=True
        )
        template = env.get_template("bench_diff_template.md")

        rendered_html = template.render(
            zip=zip(agg_values_main.items(), agg_values_curr.items()),
            method=self.method,
            currentbranch=self.current_branch,
            exceptions=self.exceptions,
        )

        with open("bench_diff.md", "w") as output_file:
            output_file.write(rendered_html)

    def get_metrics(self, path: str, res: Response) -> dict:
        time_response: float = 0
        byte_size: float = 0
        if res.is_success:
            time_response = res.elapsed.total_seconds()
            byte_size = len(res.content)
        else:
            self.exceptions.append(
                dict(
                    path=path,
                    status=res.status_code,
                    detail=res.json().get("detail"),
                    exception=res.reason_phrase,
                )
            )

        return dict(time=time_response, size=byte_size)

    def run_benchmark(self, n_requests: int) -> list:

        # time and size are the two keys to extract and make the average
        keys_to_sum = ["time", "size"]
        user_metrics: list[dict] = []

        for path in self.cleaned_paths:
            for user in self.users:
                headers = {"Authorization": f"Bearer {user.token}"}

                # list of dicts made by get_metrics()
                metrics_list = [
                    self.get_metrics(
                        path,
                        self.client.get(path, headers=headers),
                    )
                    for n in range(n_requests)
                ]
                # dicts with two keys -> key to sum (time, size)
                avg_metrics_user = {
                    key: round(
                        sum(metric[key] for metric in metrics_list)
                        / n_requests,
                        6,
                    )
                    for key in keys_to_sum
                }

                # final list of flatten dicts
                user_metrics.append(
                    dict(
                        path=path,
                        username=user.name.split("@")[0],  # remove domain
                        time=round(
                            avg_metrics_user.get("time") * 1000, 1
                        ),  # millisecond
                        size=round(
                            avg_metrics_user.get("size") / 1000, 1
                        ),  # kbyte
                    )
                )

        with open("bench.json", "w") as f:
            json.dump(user_metrics, f)

        return user_metrics


if __name__ == "__main__":

    if len(sys.argv[1:]) == 1:
        current_branch = sys.argv[1]
    else:
        current_branch = "current-branch"

    benchmark = Benchmark(
        method="GET",
        cleaned_paths=API_PATHS,  # get_clean_API_paths(),
        users=USERS,
        current_branch=current_branch,
    )
    user_metrics = benchmark.run_benchmark(N_REQUESTS)

    agg_values_curr = benchmark.aggregate_on_path(user_metrics)

    benchmark.to_html(agg_values_curr, N_REQUESTS)

    # get the bench_diff.json from the bechmark-api branch
    url = (
        "https://raw.githubusercontent.com/fractal-analytics-platform/"
        "fractal-server/benchmark-api/benchmarks/bench.json"
    )
    response = httpx.get(url)
    if response.is_error:
        raise ValueError(
            f"GET {url} returned status code {response.status_code}.\n"
            "Does bench.json exist in the benchmark-api branch?"
        )
    try:
        agg_values_main = benchmark.aggregate_on_path(response.json())
    except BaseException:
        raise ValueError(
            f"Cannot decode response-body JSON for GET call to {url}"
            f"(which returned status code {response.status_code}).\n"
            "Does bench.json exist in the benchmark-api branch?"
        )

    benchmark.make_md_diff(agg_values_main, agg_values_curr)
