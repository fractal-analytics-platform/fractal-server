"""
Translate the `#SBATCH` preamble of a fractal submission script into a
`slurmrestd` job description.

`slurmrestd` ignores `#SBATCH` lines in the submitted script, so every
option must become a JSON field.

JSON fields follow the `SLURM_REST_API_VERSION` (`client.py`) of the
`slurmrestd` OpenAPI plugin. All mappings were checked against `sbatch` on a
real cluster (Slurm 25.11, `v0.0.44`): jobs submitted both ways get the same
`scontrol show job` values, also for node lists and ranges in `--nodelist`
and `--exclude`.

Why a hand-written mapping: no SLURM tool converts `sbatch` options into a
REST job description, and options and JSON fields differ in names and units
(`--mem` -> `memory_per_node` in MB, `--time` -> `time_limit` in minutes,
`--ntasks` -> `tasks`, ...), so a generic name-based translation would be
wrong. The lines are a closed set (`SlurmConfig.to_sbatch_preamble` plus the
runner) except for admin `extra_lines`. Gaps fail loudly: unknown options
raise here (also at runner start, for the resource `extra_lines`), unknown
JSON fields are rejected at submission, and a unit test covers every
`SlurmConfig` option.
"""

from typing import Any

from fractal_server.runner.config.slurm_mem_to_MB import slurm_mem_to_MB
from fractal_server.runner.exceptions import SlurmConfigError

SBATCH_PREFIX = "#SBATCH"

SHORT_OPTIONS = {
    "A": "account",
    "C": "constraint",
    "D": "chdir",
    "G": "gpus",
    "J": "job-name",
    "N": "nodes",
    "c": "cpus-per-task",
    "e": "error",
    "n": "ntasks",
    "o": "output",
    "p": "partition",
    "q": "qos",
    "t": "time",
    "w": "nodelist",
    "x": "exclude",
}
LONG_ALIASES = {"err": "error", "out": "output"}


def _set_number(value: int) -> dict[str, Any]:
    return {"set": True, "number": value}


def slurm_time_to_minutes(value: str) -> int:
    """
    Convert a SLURM time string into minutes (seconds are rounded up).

    Accepted formats: `M`, `M:S`, `H:M:S`, `D-H`, `D-H:M`, `D-H:M:S`.
    """
    try:
        days = 0
        if "-" in value:
            days_str, value = value.split("-", 1)
            days = int(days_str)
            hours, minutes, seconds = (
                [int(x) for x in value.split(":")] + [0, 0]
            )[:3]
        else:
            parts = [int(x) for x in value.split(":")]
            if len(parts) == 1:
                hours, minutes, seconds = 0, parts[0], 0
            elif len(parts) == 2:
                hours, minutes, seconds = 0, parts[0], parts[1]
            elif len(parts) == 3:
                hours, minutes, seconds = parts
            else:
                raise ValueError
    except ValueError:
        raise SlurmConfigError(f"Invalid SLURM time value '{value}'.")
    return days * 24 * 60 + hours * 60 + minutes + (1 if seconds > 0 else 0)


def _mem_MB(value: str) -> int:
    # `srun`-style "MB" suffix is also accepted by sbatch
    if value.endswith("B"):
        value = value[:-1]
    return slurm_mem_to_MB(value)


def _parse_sbatch_line(line: str) -> tuple[str, str]:
    option = line[len(SBATCH_PREFIX) :].strip()
    if option.startswith("--"):
        option = option[2:]
        if "=" in option:
            key, value = option.split("=", 1)
        else:
            key, _, value = option.partition(" ")
        key = LONG_ALIASES.get(key, key)
    elif option.startswith("-") and len(option) > 1:
        short = option[1]
        if short not in SHORT_OPTIONS:
            raise SlurmConfigError(f"Unsupported SBATCH option in '{line}'.")
        key = SHORT_OPTIONS[short]
        value = option[2:].lstrip(" =")
    else:
        raise SlurmConfigError(f"Cannot parse SBATCH line '{line}'.")
    value = value.strip()
    if not value:
        raise SlurmConfigError(f"Missing value in SBATCH line '{line}'.")
    return key, value


def _apply_option(job: dict[str, Any], key: str, value: str) -> None:
    match key:
        case "partition":
            job["partition"] = value
        case "account":
            job["account"] = value
        case "job-name":
            job["name"] = value
        case "ntasks":
            job["tasks"] = int(value)
        case "cpus-per-task":
            job["cpus_per_task"] = int(value)
        case "mem":
            job["memory_per_node"] = _set_number(_mem_MB(value))
        case "mem-per-cpu":
            job["memory_per_cpu"] = _set_number(_mem_MB(value))
        case "time":
            job["time_limit"] = _set_number(slurm_time_to_minutes(value))
        case "output":
            job["standard_output"] = value
        case "error":
            job["standard_error"] = value
        case "chdir":
            job["current_working_directory"] = value
        case "constraint":
            job["constraints"] = value
        case "gres":
            job["tres_per_node"] = f"gres/{value}"
        case "gpus":
            job["tres_per_job"] = f"gres/gpu:{value}"
        case "exclude":
            job["excluded_nodes"] = [value]
        case "nodelist":
            job["required_nodes"] = [value]
        case "nodes":
            # `--nodes=N` means exactly N nodes
            job["minimum_nodes"] = job["maximum_nodes"] = int(value)
        case "qos":
            job["qos"] = value
        case _:
            raise SlurmConfigError(
                f"SBATCH option '--{key}' is not supported by the SLURM REST "
                "runner."
            )


def validate_sbatch_lines(lines: list[str]) -> None:
    """
    Raise `SlurmConfigError` if any `#SBATCH` line cannot be translated.
    """
    for line in lines:
        if line.startswith(SBATCH_PREFIX):
            _apply_option({}, *_parse_sbatch_line(line))


def build_job_description(
    *,
    script: str,
    environment: list[str],
) -> dict[str, Any]:
    """
    Build the `job` part of a `POST /job/submit` request.

    Args:
        script: Submission script, including its `#SBATCH` lines.
        environment:
            Base environment (`KEY=value` items). `slurmrestd` requires it,
            and jobs do not inherit any login environment.
    """
    job: dict[str, Any] = {}
    for line in script.splitlines():
        if line.startswith(SBATCH_PREFIX):
            key, value = _parse_sbatch_line(line)
            _apply_option(job, key, value)

    # As `sbatch` does ("can't run 1 processes on 2 nodes, setting nnodes to
    # 1"), while `slurmrestd` would allocate the extra nodes
    if job.get("minimum_nodes", 0) > job.get("tasks", float("inf")):
        job["minimum_nodes"] = job["maximum_nodes"] = job["tasks"]

    job_environment = list(environment)
    # Unlike with sbatch, SLURM_CPUS_PER_TASK is not set in REST-submitted
    # jobs, but fractal `srun` lines rely on it.
    if "cpus_per_task" in job:
        job_environment.append(f"SLURM_CPUS_PER_TASK={job['cpus_per_task']}")
    job["environment"] = job_environment
    return job
