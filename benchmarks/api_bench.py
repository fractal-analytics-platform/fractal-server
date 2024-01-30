import json
import re
from datetime import datetime
from typing import Optional

import httpx
from httpx import Client
from httpx import Response
from jinja2 import Environment
from jinja2 import FileSystemLoader
from pydantic import BaseModel


class UserBench(BaseModel):
    name: str
    password: str
    token: Optional[str]


USERS = [
    UserBench(  # nosec
        name="user-vanilla@example.org", password="user-vanilla-pwd"
    ),
    UserBench(  # nosec
        name="user-power@example.org", password="user-power-pwd"
    ),
]

N_REQUESTS = 25


def get_cleaned_paths() -> list:
    swagger_url = "http://127.0.0.1:8000/openapi.json"

    response = httpx.get(swagger_url)

    if response.status_code == 200:
        swagger_data = response.json()

        paths = [
            path
            for path, path_data in swagger_data.get("paths", {}).items()
            if "get" in path_data
        ]
        patterns = [
            re.compile(r"/api/v1/task/"),
            re.compile(r"/auth/"),
            re.compile(r"/admin/"),
            re.compile(r"/status/"),
            re.compile(r"/download/"),
            re.compile(r"/export/"),
            re.compile(r"/import/"),
            re.compile(r"/export_history/"),
            re.compile(r"/workflow/"),
            re.compile(r"\{.*?\}"),
        ]

        cleaned_paths = [
            path
            for path in paths
            if not any(pattern.search(path) for pattern in patterns)
        ]
    return cleaned_paths


class Benchmark:
    def __init__(self, method: str, cleaned_paths: list, users: list):

        self.method = method
        self.cleaned_paths = cleaned_paths
        self.users = users
        self.client = Client(base_url="http://localhost:8000")

        for user in self.users:
            user.token = (
                self.client.post(
                    "/auth/token/login/",
                    data=dict(username=user.name, password=user.password),
                )
                .json()
                .get("access_token")
            )

    def aggregate_on_path(self, user_metrics: list) -> None:

        aggregated_values = {}

        # for each dict in the list we aggregate on "path" key
        for bench in user_metrics:
            key_to_aggregate = bench["path"]
            if key_to_aggregate not in aggregated_values:
                aggregated_values[key_to_aggregate] = []
            # remove path key from dict because
            # it is the key now
            del bench["path"]

            aggregated_values[key_to_aggregate].append(bench)
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

    def make_html_diff(self, agg_values_main: dict, agg_values_curr: dict):

        env = Environment(
            loader=FileSystemLoader(searchpath="./templates"), autoescape=True
        )
        template = env.get_template("bench_diff_template.html")

        rendered_html = template.render(
            zip=zip(agg_values_main.items(), agg_values_curr.items()),
            method=self.method,
        )

        with open("bench_diff.html", "w") as output_file:
            output_file.write(rendered_html)

    def get_metrics(self, res: Response) -> dict:

        time_response = res.elapsed.total_seconds()
        byte_size = len(res.content)

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
                    self.get_metrics(self.client.get(path, headers=headers))
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

    benchmark = Benchmark(
        method="GET", cleaned_paths=get_cleaned_paths(), users=USERS
    )
    user_metrics = benchmark.run_benchmark(N_REQUESTS)

    agg_values_curr = benchmark.aggregate_on_path(user_metrics)

    benchmark.to_html(agg_values_curr, N_REQUESTS)

    # get the bench_diff.json from the bechmark-api branch
    json_diff = httpx.get(
        "https://raw.githubusercontent.com/fractal-analytics-platform/"
        "fractal-server/benchmark-api/benchmarks/bench.json"
    )
    print(json_diff.status_code)
    agg_values_main = benchmark.aggregate_on_path(json_diff.json())

    benchmark.make_html_diff(agg_values_main, agg_values_curr)
