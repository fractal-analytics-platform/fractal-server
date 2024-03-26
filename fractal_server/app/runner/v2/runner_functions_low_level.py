"""
Homogeneous set of Python functions that wrap executable commands.
Each function in this module should have
1. An input argument `input_kwargs`
2. An input argument `task`
3. A `dict[str, Any]` return type, that will be validated downstram with
    either TaskOutput or InitTaskOutput
... TBD
"""
import json
import shlex
import subprocess  # nosec
from pathlib import Path
from typing import Any

from fractal_server.app.runner.v2.models import Task
from fractal_server.app.runner.v2.models import TaskV1


def _run_single_non_parallel_task(
    input_kwargs: dict[str, Any],
    task: Task,
) -> dict[str, Any]:
    args_json_path = "/tmp/args.json"  # nosec
    out_json_path = "/tmp/out.json"  # nosec

    with open(args_json_path, "w") as f:
        json.dump(input_kwargs, f, indent=2)

    if task.command_non_parallel is None:
        raise
    else:
        cmd = (
            f"{task.command_non_parallel} "
            f"--args-json {args_json_path} "
            f"--out-json {out_json_path}"
        )
        subprocess.run(  # nosec
            shlex.split(cmd),
            capture_output=True,
            check=False,
            encoding="utf8",
        )

    if not Path(out_json_path).exists():
        return None

    with open(out_json_path, "r") as f:
        task_output = json.load(f)

    if task_output == {}:
        return None

    return task_output


def _run_single_parallel_task(
    input_kwargs: dict[str, Any],
    task: Task,
) -> dict[str, Any]:
    args_json_path = "/tmp/args-parallel.json"  # nosec
    out_json_path = "/tmp/out-parallel.json"  # nosec

    with open(args_json_path, "w") as f:
        json.dump(input_kwargs, f, indent=2)

    if task.command_parallel is None:
        raise
    else:
        cmd = (
            f"{task.command_parallel} "
            f"--args-json {args_json_path} "
            f"--out-json {out_json_path}"
        )
        subprocess.run(  # nosec
            shlex.split(cmd),
            capture_output=True,
            check=False,
            encoding="utf8",
        )

    if not Path(out_json_path).exists():
        return None

    with open(out_json_path, "r") as f:
        task_output = json.load(f)

    if task_output == {}:
        return None

    return task_output


def _run_single_parallel_task_v1(
    input_kwargs: dict[str, Any],
    task: TaskV1,
) -> dict[str, Any]:
    args_json_path = "/tmp/args-parallel-v1.json"  # nosec
    out_json_path = "/tmp/out-parallel-v1.json"  # nosec

    with open(args_json_path, "w") as f:
        json.dump(input_kwargs, f, indent=2)

    if task.command is None:
        raise
    else:
        cmd = (
            f"{task.command} "
            f"--args-json {args_json_path} "
            f"--out-json {out_json_path}"
        )
        subprocess.run(  # nosec
            shlex.split(cmd),
            capture_output=True,
            check=False,
            encoding="utf8",
        )

    return None
