import json
from pathlib import Path
from typing import Any
from typing import Optional

import pytest
from devtools import debug
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from fractal_server.app.runner.executors.slurm_common._slurm_config import (
    SlurmConfigError,
)
from fractal_server.app.runner.executors.slurm_common._submit_setup import (
    _slurm_submit_setup,
)
from fractal_server.app.runner.executors.slurm_common.get_slurm_config import (
    get_slurm_config,
)


class TaskV2Mock(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: int = 1
    name: str = "name_t2"
    source: str = "source_t2"
    input_types: dict[str, bool] = Field(default_factory=dict)
    output_types: dict[str, bool] = Field(default_factory=dict)

    command_non_parallel: Optional[str] = "cmd_t2_non_parallel"
    command_parallel: Optional[str] = None
    meta_parallel: Optional[dict[str, Any]] = Field(default_factory=dict)
    meta_non_parallel: Optional[dict[str, Any]] = Field(default_factory=dict)
    type: Optional[str] = None


class WorkflowTaskV2Mock(BaseModel):
    model_config = ConfigDict(extra="forbid")
    args_non_parallel: dict[str, Any] = Field(default_factory=dict)
    args_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_non_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: dict[str, Any] = Field(default_factory=dict)
    meta_parallel: Optional[dict[str, Any]] = Field(None)
    meta_non_parallel: Optional[dict[str, Any]] = Field(None)
    task: TaskV2Mock
    type_filters: dict[str, bool] = Field(default_factory=dict)
    order: int = 0
    id: int = 1
    workflow_id: int = 0
    task_id: int

    @model_validator(mode="before")
    @classmethod
    def merge_meta(cls, values):
        task_meta_parallel = values["task"].meta_parallel
        if task_meta_parallel:
            values["meta_parallel"] = {
                **task_meta_parallel,
                **values["meta_parallel"],
            }
        task_meta_non_parallel = values["task"].meta_non_parallel
        if task_meta_non_parallel:
            values["meta_non_parallel"] = {
                **task_meta_non_parallel,
                **values["meta_non_parallel"],
            }
        return values


def test_get_slurm_config(tmp_path: Path):
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
    USER_LOCAL_EXPORTS = {"SOME_CACHE_DIR": "SOME_CACHE_DIR"}

    original_slurm_config = {
        "default_slurm_config": {
            "partition": "main",
            "mem": "1G",
            "account": DEFAULT_ACCOUNT,
            "extra_lines": DEFAULT_EXTRA_LINES,
        },
        "gpu_slurm_config": {
            "partition": GPU_PARTITION,
            "mem": "1G",
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
        "user_local_exports": USER_LOCAL_EXPORTS,
    }

    config_path = tmp_path / "slurm_config.json"
    with config_path.open("w") as f:
        json.dump(original_slurm_config, f)

    # Create Task
    CPUS_PER_TASK = 1
    MEM = 1
    CUSTOM_GRES = "my-custom-gres-from-task"
    meta_non_parallel = dict(
        cpus_per_task=CPUS_PER_TASK,
        mem=MEM,
        needs_gpu=False,
        gres=CUSTOM_GRES,
        extra_lines=["a", "b", "c", "d"],
    )
    mytask = TaskV2Mock(
        name="My beautiful task",
        command_non_parallel="python something.py",
        meta_non_parallel=meta_non_parallel,
    )

    # Create WorkflowTask
    CPUS_PER_TASK_OVERRIDE = 2
    CUSTOM_CONSTRAINT = "my-custom-constraint-from-wftask"
    CUSTOM_EXTRA_LINES = ["export VAR1=VALUE1", "export VAR2=VALUE2"]
    MEM_OVERRIDE = "1G"
    MEM_OVERRIDE_MB = 1000
    meta_non_parallel = dict(
        cpus_per_task=CPUS_PER_TASK_OVERRIDE,
        mem=MEM_OVERRIDE,
        needs_gpu=True,
        constraint=CUSTOM_CONSTRAINT,
        extra_lines=CUSTOM_EXTRA_LINES,
    )
    mywftask = WorkflowTaskV2Mock(
        task=mytask,
        task_id=mytask.id,
        args_non_parallel=dict(message="test"),
        meta_non_parallel=meta_non_parallel,
    )

    # Call get_slurm_config
    slurm_config = get_slurm_config(
        wftask=mywftask,
        config_path=config_path,
        which_type="non_parallel",
    )

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
    assert "time" not in slurm_config.model_dump(exclude_unset=True).keys()
    # Check that extra_lines from WorkflowTask.meta and config_path
    # are combined together, and that repeated elements were removed
    assert len(slurm_config.extra_lines) == 3
    assert len(slurm_config.extra_lines) == len(set(slurm_config.extra_lines))
    # Check value of user_local_exports
    assert slurm_config.user_local_exports == USER_LOCAL_EXPORTS


def test_get_slurm_config_fail(tmp_path):
    slurm_config = {
        "default_slurm_config": {
            "partition": "main",
            "cpus_per_task": 1,
            "mem": "1G",
        },
        "gpu_slurm_config": {
            "partition": "main",
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

    # Valid
    config_path_valid = tmp_path / "slurm_config_valid.json"
    with config_path_valid.open("w") as f:
        json.dump(slurm_config, f)
    get_slurm_config(
        wftask=WorkflowTaskV2Mock(
            task=TaskV2Mock(),
            task_id=TaskV2Mock().id,
            meta_non_parallel={},
        ),
        config_path=config_path_valid,
        which_type="non_parallel",
    )

    # Invalid
    slurm_config["INVALID_KEY"] = "something"
    config_path_invalid = tmp_path / "slurm_config_invalid.json"
    with config_path_invalid.open("w") as f:
        json.dump(slurm_config, f)
    with pytest.raises(
        SlurmConfigError, match="Extra inputs are not permitted"
    ) as e:
        get_slurm_config(
            wftask=WorkflowTaskV2Mock(
                task=TaskV2Mock(),
                task_id=TaskV2Mock().id,
                meta_non_parallel={},
            ),
            config_path=config_path_invalid,
            which_type="non_parallel",
        )
    debug(e.value)


def test_get_slurm_config_wftask_meta_none(tmp_path):
    """
    Similar to test_get_slurm_config, but wftask has meta=None.
    """

    # Write gloabl variables into JSON config file
    GPU_PARTITION = "gpu-partition"
    GPU_DEFAULT_GRES = "gpu-default-gres"
    GPU_DEFAULT_CONSTRAINT = "gpu-default-constraint"
    DEFAULT_ACCOUNT = "default-account"
    DEFAULT_EXTRA_LINES = ["#SBATCH --option=value", "export VAR1=VALUE1"]
    USER_LOCAL_EXPORTS = {"SOME_CACHE_DIR": "SOME_CACHE_DIR"}

    slurm_config = {
        "default_slurm_config": {
            "partition": "main",
            "mem": "1G",
            "account": DEFAULT_ACCOUNT,
            "extra_lines": DEFAULT_EXTRA_LINES,
        },
        "gpu_slurm_config": {
            "partition": GPU_PARTITION,
            "mem": "1G",
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
        "user_local_exports": USER_LOCAL_EXPORTS,
    }
    config_path = tmp_path / "slurm_config.json"
    with config_path.open("w") as f:
        json.dump(slurm_config, f)

    # Create WorkflowTask
    CPUS_PER_TASK_OVERRIDE = 2
    CUSTOM_CONSTRAINT = "my-custom-constraint-from-wftask"
    CUSTOM_EXTRA_LINES = ["export VAR1=VALUE1", "export VAR2=VALUE2"]
    MEM_OVERRIDE = "1G"
    MEM_OVERRIDE_MB = 1000
    meta_non_parallel = dict(
        cpus_per_task=CPUS_PER_TASK_OVERRIDE,
        mem=MEM_OVERRIDE,
        needs_gpu=True,
        constraint=CUSTOM_CONSTRAINT,
        extra_lines=CUSTOM_EXTRA_LINES,
    )
    mywftask = WorkflowTaskV2Mock(
        task=TaskV2Mock(meta_non_parallel=None),
        task_id=TaskV2Mock(meta_non_parallel=None).id,
        args_non_parallel=dict(message="test"),
        meta_non_parallel=meta_non_parallel,
    )
    debug(mywftask)

    # Call get_slurm_config
    slurm_config = get_slurm_config(
        wftask=mywftask,
        config_path=config_path,
        which_type="non_parallel",
    )
    debug(slurm_config)

    # Check that WorkflowTask.meta takes priority over WorkflowTask.Task.meta
    assert slurm_config.cpus_per_task == CPUS_PER_TASK_OVERRIDE
    assert slurm_config.mem_per_task_MB == MEM_OVERRIDE_MB
    assert slurm_config.partition == GPU_PARTITION
    # Check that both WorkflowTask.meta and WorkflowTask.Task.meta take
    # priority over the "if_needs_gpu" key-value pair in slurm_config.json
    assert slurm_config.constraint == CUSTOM_CONSTRAINT
    # Check that some optional attributes are set/unset correctly
    assert slurm_config.job_name
    assert " " not in slurm_config.job_name
    assert slurm_config.account == DEFAULT_ACCOUNT
    assert "time" not in slurm_config.model_dump(exclude_unset=True).keys()
    # Check that extra_lines from WorkflowTask.meta and config_path
    # are combined together, and that repeated elements were removed
    assert len(slurm_config.extra_lines) == 3
    assert len(slurm_config.extra_lines) == len(set(slurm_config.extra_lines))
    # Check value of user_local_exports
    assert slurm_config.user_local_exports == USER_LOCAL_EXPORTS


def test_slurm_submit_setup(
    tmp_path: Path, testdata_path: Path, override_settings_factory
):
    override_settings_factory(
        FRACTAL_SLURM_CONFIG_FILE=testdata_path / "slurm_config.json"
    )

    # No account in `wftask.meta` --> OK
    wftask = WorkflowTaskV2Mock(task=TaskV2Mock(), task_id=TaskV2Mock().id)
    slurm_config = _slurm_submit_setup(
        wftask=wftask,
        workflow_dir_local=tmp_path,
        workflow_dir_remote=tmp_path,
        which_type="non_parallel",
    )
    debug(slurm_config)
    assert slurm_config["slurm_config"].account is None

    # Account in `wftask.meta_non_parallel` --> fail
    wftask = WorkflowTaskV2Mock(
        meta_non_parallel=dict(key="value", account="MyFakeAccount"),
        task=TaskV2Mock(),
        task_id=TaskV2Mock().id,
    )
    with pytest.raises(SlurmConfigError) as e:
        _slurm_submit_setup(
            wftask=wftask,
            workflow_dir_local=tmp_path,
            workflow_dir_remote=tmp_path,
            which_type="non_parallel",
        )
    debug(e.value)
    assert "SLURM account" in str(e.value)


def test_get_slurm_config_gpu_options(tmp_path: Path):
    """
    Test that GPU-related options are only read when `needs_gpu=True`.
    """
    STANDARD_PARTITION = "main"
    GPU_PARTITION = "gpupartition"
    GPU_MEM = "20G"
    GPU_MEM_PER_TASK_MB = 20000
    GPUS = "1"
    PRE_SUBMISSION_COMMANDS = ["module load gpu"]

    slurm_config_dict = {
        "default_slurm_config": {
            "partition": STANDARD_PARTITION,
            "mem": "1G",
            "cpus_per_task": 1,
        },
        "gpu_slurm_config": {
            "partition": GPU_PARTITION,
            "mem": GPU_MEM,
            "gpus": GPUS,
            "pre_submission_commands": PRE_SUBMISSION_COMMANDS,
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
    config_path = tmp_path / "slurm_config.json"
    with config_path.open("w") as f:
        json.dump(slurm_config_dict, f)

    # In absence of `needs_gpu`, parameters in `gpu_slurm_config` are not used
    mywftask = WorkflowTaskV2Mock(task=TaskV2Mock(), task_id=TaskV2Mock().id)
    slurm_config = get_slurm_config(
        wftask=mywftask,
        config_path=config_path,
        which_type="non_parallel",
    )
    assert slurm_config.partition == STANDARD_PARTITION
    assert slurm_config.gpus is None
    assert slurm_config.pre_submission_commands == []

    # When `needs_gpu` is set, parameters in `gpu_slurm_config` are used
    mywftask = WorkflowTaskV2Mock(
        meta_non_parallel=dict(needs_gpu=True),
        task=TaskV2Mock(),
        task_id=TaskV2Mock().id,
    )
    slurm_config = get_slurm_config(
        wftask=mywftask,
        config_path=config_path,
        which_type="non_parallel",
    )
    assert slurm_config.partition == GPU_PARTITION
    assert slurm_config.gpus == GPUS
    assert slurm_config.mem_per_task_MB == GPU_MEM_PER_TASK_MB
    assert slurm_config.pre_submission_commands == PRE_SUBMISSION_COMMANDS
