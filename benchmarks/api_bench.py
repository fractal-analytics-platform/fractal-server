import json
import re
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
    token: Optional[str] = None


USERS = [
    UserBench(name="vanilla@example.org", password="vanilla-pwd"),  # nosec
    UserBench(name="power@example.org", password="power-pwd"),  # nosec
    UserBench(name="dataset@example.org", password="dataset-pwd"),  # nosec
    UserBench(name="project@example.org", password="project-pwd"),  # nosec
    UserBench(name="job@example.org", password="job-pwd"),  # nosec
]


N_REQUESTS = 25

ENDPOINTS = [
    dict(verb="GET", path="/api/alive/"),
    dict(verb="GET", path="/api/v2/job/"),
    dict(verb="GET", path="/api/v2/project/"),
    dict(verb="GET", path="/api/v2/task/"),
    dict(verb="GET", path="/api/v2/task-group/"),
    dict(
        verb="POST",
        path="/api/v2/project/$project_id$/dataset/$dataset_id$/images/query/",
    ),
    dict(verb="GET", path="/auth/current-user/"),
    dict(
        verb="POST",
        path="/auth/token/login/",
        data=dict(username=USERS[0].name, password=USERS[0].password),
    ),
]


class Benchmark:
    def __init__(
        self,
        endpoints: list[dict[str, str]],
        users: list[UserBench],
    ):
        self.endpoints = endpoints
        self.users = users
        self.client = Client(base_url=FRACTAL_SERVER_URL)
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

    def aggregate_on_path(
        self, user_metrics: list[dict[str, Any]]
    ) -> dict[str, list]:
        """
        Given a list of benchmarks, aggregate them on their "path" keys.
        """
        aggregated_values = {}
        for bench in user_metrics:
            current_path = bench.pop("path")
            current_verb = bench.pop("verb")
            key = current_verb + " " + current_path
            if key not in aggregated_values.keys():
                aggregated_values[key] = []
            aggregated_values[key].append(bench)
        return aggregated_values

    def to_html(self, aggregated_values: dict, n_requests: int):
        env = Environment(
            loader=FileSystemLoader(searchpath="./templates"), autoescape=True
        )
        template = env.get_template("bench_template.html")

        rendered_html = template.render(
            user_metrics=aggregated_values,
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

        rendered_md = template.render(
            zip=zip(agg_values_main.items(), agg_values_curr.items()),
            exceptions=self.exceptions,
        )

        with open("bench_diff.md", "w") as output_file:
            output_file.write(rendered_md)

    def get_metrics(self, user, path: str, res: Response) -> dict:
        time_response: float = 0
        byte_size: float = 0
        if res.is_success:
            time_response = res.elapsed.total_seconds()
            byte_size = len(res.content)
        else:
            try:
                detail = res.json().get("detail")
            except json.JSONDecodeError:
                detail = "Cannot parse response JSON"
            self.exceptions.append(
                dict(
                    user=user,
                    path=path,
                    status=res.status_code,
                    detail=detail,
                    exception=res.reason_phrase,
                )
            )
            raise Exception(self.exceptions)

        return dict(time=time_response, size=byte_size)

    def _replace_path_params(self, headers: dict, path: str):
        """ """

        pattern = r"\$(.*?)\$"
        matches = re.findall(pattern, path)
        if matches:
            project_id = self.client.get(
                "/api/v2/project/", headers=headers
            ).json()[0]["id"]
            dataset_id = self.client.get(
                "/api/v2/dataset/", headers=headers
            ).json()[0]["id"]
            id_list = iter([str(project_id), str(dataset_id)])
            updated_path = re.sub(pattern, lambda x: next(id_list), path)
        else:
            updated_path = path
        return updated_path

    def make_user_metrics(
        self,
        endpoint: dict[str, str],
        headers: dict[str, str],
        user: UserBench,
        n_requests: int,
        keys_to_sum: list[str],
    ):
        # list of dicts made by get_metrics()
        verb = endpoint.get("verb")
        path = endpoint.get("path")
        if verb == "GET":
            metrics_list = [
                self.get_metrics(
                    user,
                    path,
                    self.client.get(path, headers=headers),
                )
                for n in range(n_requests)
            ]
        elif verb == "POST":
            path = self._replace_path_params(headers, path)
            request_json = endpoint.get("json")
            request_data = endpoint.get("data")
            metrics_list = [
                self.get_metrics(
                    user,
                    path,
                    self.client.post(
                        path,
                        headers=headers,
                        json=request_json,
                        data=request_data,
                    ),
                )
                for _ in range(n_requests)
            ]

        # dicts with two keys -> key to sum (time, size)
        avg_metrics_user = {
            key: round(
                sum(metric[key] for metric in metrics_list) / n_requests,
                6,
            )
            for key in keys_to_sum
        }
        # final list of flatten dicts
        return dict(
            path=path,
            verb=verb,
            username=user.name.split("@")[0],  # remove domain
            time=round(avg_metrics_user.get("time") * 1000, 1),  # millisecond
            size=round(avg_metrics_user.get("size") / 1000, 1),  # kbyte
        )

    def run_benchmark(self, n_requests: int) -> list:
        # time and size are the two keys to extract and make the average
        keys_to_sum = ["time", "size"]
        user_metrics: list[dict] = []

        for endpoint in self.endpoints:
            for user in self.users:
                headers = {"Authorization": f"Bearer {user.token}"}
                if (
                    endpoint["verb"] == "POST"
                    and user.name != "dataset@example.org"
                ):
                    pass
                else:
                    user_metrics.append(
                        self.make_user_metrics(
                            endpoint, headers, user, n_requests, keys_to_sum
                        )
                    )

        with open("bench.json", "w") as f:
            json.dump(user_metrics, f)

        return user_metrics


if __name__ == "__main__":
    benchmark = Benchmark(
        endpoints=ENDPOINTS,
        users=USERS,
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
