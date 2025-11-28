from typing import Any

from pydantic import BaseModel
from pydantic import Field

from fractal_server.runner.config import JobRunnerConfigSLURM
from fractal_server.runner.executors.slurm_common.get_slurm_config import (
    _get_slurm_config_internal,
)
from fractal_server.runner.executors.slurm_common.slurm_config import (
    SlurmConfig,
)


class MockTask(BaseModel):
    name: str = "task-name"


class WorkflowTaskMock(BaseModel):
    task: MockTask = Field(default_factory=MockTask)
    meta_parallel: dict[str, Any] | None = Field(None)
    meta_non_parallel: dict[str, Any] | None = Field(None)


def test_get_slurm_config_internal():
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

    shared_slurm_config = JobRunnerConfigSLURM(
        default_slurm_config={
            "partition": "main",
            "mem": "1G",
            "account": DEFAULT_ACCOUNT,
            "extra_lines": DEFAULT_EXTRA_LINES,
        },
        gpu_slurm_config={
            "partition": GPU_PARTITION,
            "mem": "1G",
            "gres": GPU_DEFAULT_GRES,
            "constraint": GPU_DEFAULT_CONSTRAINT,
        },
        batching_config={
            "target_cpus_per_job": 10,
            "max_cpus_per_job": 12,
            "target_mem_per_job": 10,
            "max_mem_per_job": 12,
            "target_num_jobs": 5,
            "max_num_jobs": 10,
        },
        user_local_exports=USER_LOCAL_EXPORTS,
    )

    # Create WorkflowTask
    CUSTOM_GRES = "my-custom-gres-from-task"
    CPUS_PER_TASK_OVERRIDE = 2
    CUSTOM_CONSTRAINT = "my-custom-constraint-from-wftask"
    CUSTOM_EXTRA_LINES = ["export VAR1=VALUE1", "export VAR2=VALUE2"]
    MEM_OVERRIDE = "1G"
    MEM_OVERRIDE_MB = 1000
    meta_non_parallel = dict(
        cpus_per_task=CPUS_PER_TASK_OVERRIDE,
        mem=MEM_OVERRIDE,
        needs_gpu=True,
        gres=CUSTOM_GRES,
        constraint=CUSTOM_CONSTRAINT,
        extra_lines=CUSTOM_EXTRA_LINES,
    )
    mywftask = WorkflowTaskMock(meta_non_parallel=meta_non_parallel)

    # Call get_slurm_config_internal
    slurm_config = _get_slurm_config_internal(
        shared_config=shared_slurm_config,
        wftask=mywftask,
        which_type="non_parallel",
    )

    # Check that WorkflowTask.meta takes priority over `shared_config`
    assert slurm_config.cpus_per_task == CPUS_PER_TASK_OVERRIDE
    assert slurm_config.mem_per_task_MB == MEM_OVERRIDE_MB
    assert slurm_config.partition == GPU_PARTITION

    # Check that both WorkflowTask.meta takes priority over the
    # "if_needs_gpu" key-value pair `shared_config`
    assert slurm_config.gres == CUSTOM_GRES
    assert slurm_config.constraint == CUSTOM_CONSTRAINT

    # Check that some optional attributes are set/unset correctly
    assert slurm_config.job_name
    assert " " not in slurm_config.job_name
    assert slurm_config.account == DEFAULT_ACCOUNT
    assert "time" not in slurm_config.model_dump(exclude_unset=True).keys()

    # Check that extra_lines from WorkflowTask.meta and `shared_config`
    # are combined together, and that repeated elements were removed
    assert len(slurm_config.extra_lines) == 3
    assert len(slurm_config.extra_lines) == len(set(slurm_config.extra_lines))
    # Check value of user_local_exports
    assert slurm_config.user_local_exports == USER_LOCAL_EXPORTS


def test_get_slurm_config_internal_gpu_options():
    """
    Test that GPU-related options are only read when `needs_gpu=True`.
    """
    STANDARD_PARTITION = "main"
    GPU_PARTITION = "gpupartition"
    GPU_MEM = "20G"
    GPU_MEM_PER_TASK_MB = 20000
    GPUS = "1"

    shared_slurm_config = JobRunnerConfigSLURM(
        default_slurm_config={
            "partition": STANDARD_PARTITION,
            "mem": "1G",
            "cpus_per_task": 1,
        },
        gpu_slurm_config={
            "partition": GPU_PARTITION,
            "mem": GPU_MEM,
            "gpus": GPUS,
        },
        batching_config={
            "target_cpus_per_job": 10,
            "max_cpus_per_job": 12,
            "target_mem_per_job": 10,
            "max_mem_per_job": 12,
            "target_num_jobs": 5,
            "max_num_jobs": 10,
        },
    )
    assert shared_slurm_config.user_local_exports == {}
    assert shared_slurm_config.default_slurm_config.extra_lines == []

    # In absence of `needs_gpu`, parameters in `gpu_slurm_config` are not used
    mywftask = WorkflowTaskMock()
    slurm_config = _get_slurm_config_internal(
        shared_config=shared_slurm_config,
        wftask=mywftask,
        which_type="non_parallel",
    )
    assert slurm_config.partition == STANDARD_PARTITION
    assert slurm_config.gpus is None

    # When `needs_gpu` is set, parameters in `gpu_slurm_config` are used
    mywftask = WorkflowTaskMock(meta_non_parallel=dict(needs_gpu=True))
    slurm_config = _get_slurm_config_internal(
        shared_config=shared_slurm_config,
        wftask=mywftask,
        which_type="non_parallel",
    )
    assert slurm_config.partition == GPU_PARTITION
    assert slurm_config.gpus == GPUS
    assert slurm_config.mem_per_task_MB == GPU_MEM_PER_TASK_MB


def test_SlurmConfig():
    cfg = SlurmConfig(
        partition="x",
        cpus_per_task=1,
        mem_per_task_MB=1,
        target_cpus_per_job=1,
        max_cpus_per_job=1,
        target_mem_per_job=1,
        max_mem_per_job=1,
        target_num_jobs=1,
        max_num_jobs=1,
        parallel_tasks_per_job=1,
        user_local_exports={"CELLPOSE_LOCAL_MODELS_PATH": "cellpose"},
    )

    # Without trailing slash
    preamble = cfg.to_sbatch_preamble(remote_export_dir="/cache/dir")
    assert "export CELLPOSE_LOCAL_MODELS_PATH=/cache/dir/cellpose" in preamble

    # With trailing slash
    preamble = cfg.to_sbatch_preamble(remote_export_dir="/cache/dir/")
    assert "export CELLPOSE_LOCAL_MODELS_PATH=/cache/dir/cellpose" in preamble
