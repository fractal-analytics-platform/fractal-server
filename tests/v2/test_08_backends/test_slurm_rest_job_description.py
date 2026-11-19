import json
import subprocess
import typing

import pytest

from fractal_server.runner.exceptions import SlurmConfigError
from fractal_server.runner.executors.slurm_common.slurm_config import (
    SlurmConfig,
)
from fractal_server.runner.executors.slurm_rest.job_description import OPTIONS
from fractal_server.runner.executors.slurm_rest.job_description import (
    _apply_option,
)
from fractal_server.runner.executors.slurm_rest.job_description import (
    build_job_description,
)
from fractal_server.runner.executors.slurm_rest.job_description import (
    slurm_time_to_minutes,
)
from fractal_server.runner.executors.slurm_rest.job_description import (
    validate_sbatch_lines,
)
from fractal_server.runner.executors.slurm_rest.job_script import (
    build_job_script,
)
from tests.v2._aux_runner import get_default_slurm_config

FRACTAL_SCRIPT = """#!/bin/sh
#SBATCH --partition=main
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=3
#SBATCH --mem=600M
#SBATCH --job-name=my_task
#SBATCH --time=1-02:03:04
#SBATCH --account=acc
#SBATCH --err=/remote/wf/0_par-slurm-%j.err
#SBATCH --out=/remote/wf/0_par-slurm-%j.out
#SBATCH -D /remote/wf
export FOO=/cache/foo

echo "Hostname: $(uname -n)"
"""


@pytest.mark.parametrize(
    "value,minutes",
    [
        ("10", 10),
        ("10:30", 11),
        ("02:00:00", 120),
        ("1-0", 1440),
        ("1-02:03", 1563),
        ("1-02:03:04", 1564),
    ],
)
def test_slurm_time_to_minutes(value, minutes):
    assert slurm_time_to_minutes(value) == minutes


def test_slurm_time_to_minutes_invalid():
    for value in ("1:2:3:4", "1-02:03:04:999", "1-", "UNLIMITED"):
        with pytest.raises(SlurmConfigError, match=value):
            slurm_time_to_minutes(value)


@pytest.mark.parametrize(
    "line,expected",
    [
        # As Fractal's own configuration (`slurm_mem_to_MB`, powers of 1000)
        ("--mem=600", {"memory_per_node": {"set": True, "number": 600}}),
        ("--mem=600M", {"memory_per_node": {"set": True, "number": 600}}),
        ("--mem=2G", {"memory_per_node": {"set": True, "number": 2000}}),
        ("--mem=2GB", {"memory_per_node": {"set": True, "number": 2000}}),
        ("--mem=1T", {"memory_per_node": {"set": True, "number": 1000000}}),
        ("--mem-per-cpu=2G", {"memory_per_cpu": {"set": True, "number": 2000}}),
        # Each GRES gets its `gres/` prefix, as with `sbatch`
        ("--gres=gpu:a100:1", {"tres_per_node": "gres/gpu:a100:1"}),
        (
            "--gres=gpu:a100:1,shard:2",
            {"tres_per_node": "gres/gpu:a100:1,gres/shard:2"},
        ),
    ],
)
def test_build_job_description_values(line, expected):
    job = build_job_description(script=f"#SBATCH {line}\n", environment=[])
    assert job == expected | {"environment": []}


@pytest.mark.parametrize(
    "line,expected",
    [
        ("--partition=main", {"partition": "main"}),
        ("-p main", {"partition": "main"}),
        # Unambiguous abbreviations, as with `sbatch`
        ("--err=/x.err", {"standard_error": "/x.err"}),
        # Quotes and comments, as with `sbatch`
        ('--job-name="my job"', {"name": "my job"}),
        ("--time=10 # comment", {"time_limit": {"set": True, "number": 10}}),
        # Several options on one line
        ("--qos=high --account=acc", {"qos": "high", "account": "acc"}),
    ],
)
def test_build_job_description_line_syntax(line, expected):
    job = build_job_description(script=f"#SBATCH {line}\n", environment=[])
    assert job == expected | {"environment": []}


def test_options_are_all_handled():
    # Every option accepted by the parser has a JSON mapping
    for name in OPTIONS:
        _apply_option({}, name, "1")


@pytest.mark.parametrize(
    "line",
    [
        "--mem=2X",
        "--mem=2BG",
        "--partition=main extra",
        '--job-name="unclosed',
        "--mail-type=ALL",
        # Ambiguous abbreviation (`nodelist`, `nodes`, `ntasks`)
        "--n=1",
    ],
)
def test_build_job_description_invalid_values(line):
    with pytest.raises(SlurmConfigError):
        build_job_description(script=f"#SBATCH {line}\n", environment=[])


def test_build_job_description():
    job = build_job_description(
        script=FRACTAL_SCRIPT,
        environment=["PATH=/usr/bin:/bin"],
    )
    assert job == {
        "partition": "main",
        "tasks": 2,
        "cpus_per_task": 3,
        "memory_per_node": {"set": True, "number": 600},
        "name": "my_task",
        "time_limit": {"set": True, "number": 1564},
        "account": "acc",
        "standard_error": "/remote/wf/0_par-slurm-%j.err",
        "standard_output": "/remote/wf/0_par-slurm-%j.out",
        "current_working_directory": "/remote/wf",
        "environment": ["PATH=/usr/bin:/bin", "SLURM_CPUS_PER_TASK=3"],
    }


@pytest.mark.parametrize(
    "ntasks,nodes,expected",
    [(1, 2, 1), (2, 2, 2), (4, 2, 2), (None, 3, 3)],
)
def test_build_job_description_nodes(ntasks, nodes, expected):
    """
    As `sbatch`, at most one node per task.
    """
    script = f"#SBATCH --nodes={nodes}\n"
    if ntasks is not None:
        script += f"#SBATCH --ntasks={ntasks}\n"
    job = build_job_description(script=script, environment=[])
    assert job["minimum_nodes"] == job["maximum_nodes"] == expected


@pytest.mark.parametrize("use_mem_per_cpu", [False, True])
def test_build_job_description_covers_slurm_config(use_mem_per_cpu):
    # Fails when `SlurmConfig` gains an option the REST runner cannot map
    config = get_default_slurm_config()
    for name, field in SlurmConfig.model_fields.items():
        if getattr(config, name) is None:
            is_int = int in typing.get_args(field.annotation)
            setattr(config, name, 1 if is_int else "1")
    lines = config.to_sbatch_preamble(
        remote_export_dir="/remote", use_mem_per_cpu=use_mem_per_cpu
    )
    sbatch_lines = [line for line in lines if line.startswith("#SBATCH")]
    job = build_job_description(script="\n".join(lines), environment=[])
    # One JSON field per option (two for `--nodes`), plus `environment`
    assert len(job) == len(sbatch_lines) + 2


def test_validate_sbatch_lines():
    validate_sbatch_lines(["#SBATCH --qos=high", "export X=1", "module load y"])
    with pytest.raises(SlurmConfigError, match="not supported"):
        validate_sbatch_lines(["#SBATCH --mail-type=ALL"])


def test_build_job_script_writes_files(tmp_path):
    input_path = tmp_path / "x-input.json"
    args_path = tmp_path / "x-args.json"
    copy_path = tmp_path / "x-slurm-submit.sh"
    tricky = json.dumps({"a": "$HOME `id` $(id) 'q' \"dq\" \\"}, indent=2)
    script = build_job_script(
        script=FRACTAL_SCRIPT.replace("/cache/foo", str(tmp_path)),
        files=[(str(input_path), '{"k": 1}'), (str(args_path), tricky)],
        script_copy_path=str(copy_path),
    )
    lines = script.splitlines()
    assert lines[0] == "#!/bin/sh"
    assert all(line.startswith("#SBATCH") for line in lines[1:11])
    assert lines[11] == "umask 0002"
    assert lines[12].startswith(f"cat > '{input_path}'")

    script_path = tmp_path / "submit.sh"
    script_path.write_text(script)
    subprocess.run(["sh", str(script_path)], check=True, cwd=tmp_path)
    assert json.loads(input_path.read_text()) == {"k": 1}
    assert args_path.read_text() == f"{tricky}\n"
    assert copy_path.read_text() == script


def test_build_job_script_limits():
    with pytest.raises(ValueError, match="Single quotes"):
        build_job_script(script="#!/bin/sh\n", files=[("/a'b", "")])
    with pytest.raises(ValueError, match="limit"):
        build_job_script(
            script="#!/bin/sh\n", files=[("/a", "x" * 5 * 1024 * 1024)]
        )
