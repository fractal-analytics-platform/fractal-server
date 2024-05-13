import json
import shlex
import subprocess
import time
from pathlib import Path

import pytest
import requests

import fractal_server

FRACTAL_SERVER_DIR = Path(fractal_server.__file__).parent


@pytest.mark.parametrize(
    "FRACTAL_API_V1_MODE,is_v1_enabled",
    [("include", True), ("exclude", False)],
)
async def test_exclude_v1_api(
    FRACTAL_API_V1_MODE: str,
    is_v1_enabled: bool,
    set_test_db,
):

    with (FRACTAL_SERVER_DIR / ".fractal_server.env").open("a") as f:
        f.write(f"FRACTAL_API_V1_MODE={FRACTAL_API_V1_MODE}\n")

    p = subprocess.Popen(
        shlex.split("fractalctl start --host 0.0.0.0 --port 8001"),
        start_new_session=True,
        cwd=FRACTAL_SERVER_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        encoding="utf-8",
    )
    try:
        max_iterations = 15
        it = 0
        while it < max_iterations:
            try:
                r = requests.get("http://localhost:8001/openapi.json")
                break
            except requests.exceptions.ConnectionError:
                time.sleep(0.2)
                it += 1
        if it == max_iterations:
            raise StopIteration()

        docs = json.loads(r.text)
        paths = docs["paths"].keys()
        api_v1 = [
            item
            for item in paths
            if ("v1" in item) and (item != "/admin/v2/task-v1/{task_id}/")
        ]
        if is_v1_enabled:
            assert api_v1
        else:
            assert not api_v1
    finally:
        p.terminate()
