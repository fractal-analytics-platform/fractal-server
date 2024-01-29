import json
import re
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

N_REQUESTS = 10


def get_cleaned_paths():
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
            re.compile(r"/job/"),
            re.compile(r"\{.*?\}"),
        ]

        cleaned_paths = [
            path
            for path in paths
            if not any(pattern.search(path) for pattern in patterns)
        ]
    return cleaned_paths


class Benchmark:
    def __init__(self, cleaned_paths, users):

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

    def to_html(self, json_path: str):

        with open(json_path, "r") as f:
            benchmarks = json.load(f)

        aggregated_values = {}

        for bench in benchmarks:
            key_to_aggregate = bench["path"]
            if key_to_aggregate not in aggregated_values:
                aggregated_values[key_to_aggregate] = []
            # drop a dict element
            del bench["path"]

            aggregated_values[key_to_aggregate].append(bench)
        print(aggregated_values)

        env = Environment(
            loader=FileSystemLoader(searchpath="./templates"), autoescape=True
        )
        template = env.get_template("bench_template.html")

        rendered_html = template.render(user_metrics=aggregated_values)

        with open("bench.html", "w") as output_file:
            output_file.write(rendered_html)

    def get_metrics(self, res: Response):

        time_response = res.elapsed.total_seconds()
        byte_size = len(res.content)

        return dict(time=time_response, size=byte_size)

    def run_benchmark(self):

        keys_to_sum = ["time", "size"]
        user_metrics: list = []  # dict[str, dict] = dict()

        for path in self.cleaned_paths:
            for user in self.users:
                headers = {"Authorization": f"Bearer {user.token}"}

                metrics_list = [
                    self.get_metrics(self.client.get(path, headers=headers))
                    for n in range(N_REQUESTS)
                ]
                avg_metrics_user = {
                    key: sum(metric[key] for metric in metrics_list)
                    / N_REQUESTS
                    for key in keys_to_sum
                }

                user_metrics.append(
                    dict(
                        path=path,
                        username=user.name,
                        time=avg_metrics_user.get("time"),
                        size=avg_metrics_user.get("size"),
                    )
                )
                # print(user_metrics)

        with open("bench.json", "w") as f:
            json.dump(user_metrics, f)


x = Benchmark(get_cleaned_paths(), USERS)
x.run_benchmark()
x.to_html("bench.json")
