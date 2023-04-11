import json

import pytest
from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._slurm._slurm_config import _parse_mem_value
from fractal_server.app.runner._slurm._slurm_config import (
    get_default_slurm_config,
)
from fractal_server.app.runner._slurm._slurm_config import get_slurm_config
from fractal_server.app.runner._slurm._slurm_config import SlurmConfigError


@pytest.mark.parametrize("fail", [True, False])
def test_get_slurm_config(tmp_path, fail):
    """
    Testing that:
    1. WorkflowTask.meta overrides WorkflowTask.Task.meta
    2. needs_gpu=True triggers other changes
    3. If WorkflowTask.meta includes (e.g.) "gres", then this is the actual
    value that is set (even for needs_gpu=True).
    """

    # Write gloabl variables into JSON config file
    GPU_PARTITION = "gpu-partition"
    GPU_DEFAULT_GRES = "gpu-default-gres"
    GPU_DEFAULT_CONSTRAINT = "gpu-default-constraint"
    DEFAULT_ACCOUNT = "default-account"
    DEFAULT_EXTRA_LINES = ["#SBATCH --option=value", "export VAR1=VALUE1"]
    slurm_config = {
        "default_slurm_config": {
            "partition": "main",
            "mem": "1G",
            "account": DEFAULT_ACCOUNT,
            "extra_lines": DEFAULT_EXTRA_LINES,
        },
        "gpu_slurm_config": {
            "partition": GPU_PARTITION,
            "gres": GPU_DEFAULT_GRES,
            "constraint": GPU_DEFAULT_CONSTRAINT,
        },
        "batching_config": {
            "target_cpus_per_job": 10,
            "max_cpus_per_job": 12,
            "target_mem_per_job": 10,
            "max_mem_per_job": 12,
            "target_num_jobs": 5,
            "max_num_jobs": 10,
        },
    }
    if fail:
        slurm_config["invalid_key"] = "something"
    config_path = tmp_path / "slurm_config.json"
    with config_path.open("w") as f:
        json.dump(slurm_config, f)

    # Create Task
    CPUS_PER_TASK = 1
    MEM = 1
    CUSTOM_GRES = "my-custom-gres-from-task"
    meta = dict(
        cpus_per_task=CPUS_PER_TASK,
        mem=MEM,
        needs_gpu=False,
        gres=CUSTOM_GRES,
        extra_lines=["a", "b", "c", "d"],
    )
    mytask = MockTask(
        name="My beautiful task",
        command="python something.py",
        meta=meta,
    )

    # Create WorkflowTask
    CPUS_PER_TASK_OVERRIDE = 2
    CUSTOM_CONSTRAINT = "my-custom-constraint-from-wftask"
    CUSTOM_EXTRA_LINES = ["export VAR1=VALUE1", "export VAR2=VALUE2"]
    MEM_OVERRIDE = "1G"
    MEM_OVERRIDE_MB = 1000
    meta = dict(
        cpus_per_task=CPUS_PER_TASK_OVERRIDE,
        mem=MEM_OVERRIDE,
        needs_gpu=True,
        constraint=CUSTOM_CONSTRAINT,
        extra_lines=CUSTOM_EXTRA_LINES,
    )
    mywftask = MockWorkflowTask(
        task=mytask,
        arguments=dict(message="test"),
        order=0,
        meta=meta,
    )
    debug(mywftask)
    debug(mywftask.overridden_meta)

    # Call get_slurm_config
    try:
        slurm_config = get_slurm_config(
            wftask=mywftask,
            workflow_dir=(tmp_path / "server"),
            workflow_dir_user=(tmp_path / "user"),
            config_path=config_path,
        )
        debug(slurm_config)
    except SlurmConfigError as e:
        if fail:
            debug(
                "Expected errror took place in set_slurm_config.\n"
                f"Original error:\n{str(e)}"
            )
            return
        else:
            raise e

    # Check that WorkflowTask.meta takes priority over WorkflowTask.Task.meta
    assert slurm_config.cpus_per_task == CPUS_PER_TASK_OVERRIDE
    assert slurm_config.mem_per_task_MB == MEM_OVERRIDE_MB
    assert slurm_config.partition == GPU_PARTITION
    # Check that both WorkflowTask.meta and WorkflowTask.Task.meta take
    # priority over the "if_needs_gpu" key-value pair in slurm_config.json
    assert slurm_config.gres == CUSTOM_GRES
    assert slurm_config.constraint == CUSTOM_CONSTRAINT
    # Check that some optional attributes are set/unset correctly
    assert slurm_config.job_name
    assert " " not in slurm_config.job_name
    assert slurm_config.account == DEFAULT_ACCOUNT
    assert "time" not in slurm_config.dict(exclude_unset=True).keys()
    # Check that extra_lines from WorkflowTask.overridden_meta and config_path
    # are combined together, and that repeated elements were removed
    assert len(slurm_config.extra_lines) == 3
    assert len(slurm_config.extra_lines) == len(set(slurm_config.extra_lines))


def test_to_sbatch_preamble():
    """
    Given a SlurmConfig object, test its to_sbatch_preamble method
    """

    slurm_config = get_default_slurm_config()
    debug(slurm_config)

    GRES = "some-gres"
    EXTRA_LINES = [
        "export VAR2=2",
        "#SBATCH --optionA=valueA",
        "export VAR1=1",
        "#SBATCH --optionB=valueB",
    ]
    MEM_SINGLE_TASK_MB = 100

    slurm_config.mem_per_task_MB = MEM_SINGLE_TASK_MB
    slurm_config.parallel_tasks_per_job = 3
    slurm_config.tasks_per_job = 5
    slurm_config.cpus_per_task = 2
    slurm_config.gres = GRES
    slurm_config.extra_lines = EXTRA_LINES
    preamble = slurm_config.to_sbatch_preamble()
    debug(preamble)

    assert preamble[0] == "#!/bin/sh"
    assert f"#SBATCH --gres={GRES}" in preamble
    for line in EXTRA_LINES:
        assert line in preamble
    MEM = MEM_SINGLE_TASK_MB * slurm_config.parallel_tasks_per_job
    assert f"#SBATCH --mem={MEM}M" in preamble


def test_parse_mem_value():

    # Successful calls
    assert _parse_mem_value(99) == 99
    assert _parse_mem_value("99") == 99
    assert _parse_mem_value("99M") == 99
    assert _parse_mem_value("2G") == 2000
    assert _parse_mem_value("3T") == 3000000

    # Failures
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("100 M")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("100K")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("M10M")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("10-M")
    with pytest.raises(SlurmConfigError):
        _parse_mem_value("2.5G")
