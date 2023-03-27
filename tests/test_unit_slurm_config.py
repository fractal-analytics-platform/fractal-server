import json

from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._grouped_slurm._slurm_config import (
    set_slurm_config,
)
from fractal_server.app.runner.common import TaskParameters


def test_set_slurm_config(tmp_path):
    """
    Testing that:
    1. WorkflowTask.meta overrides WorkflowTask.Task.meta
    2. needs_gpu=True triggers other changes
    3. If WorkflowTask.meta includes (e.g.) "gres", then this is the actual
    value that is set (even for needs_gpu=True).
    """

    slurm_config = {
        "partition": "main",
        "cpus_per_job": {
            "target": 10,
            "max": 10,
        },
        "mem_per_job": {
            "target": 10,
            "max": 10,
        },
        "number_of_jobs": {
            "target": 10,
            "max": 10,
        },
        "if_needs_gpu": {
            # Possible overrides: partition, gres, constraint
            "partition": "gpu",
            "gres": "gpu:1",
            "constraint": "gpuram32gb",
        },
    }
    config_path = tmp_path / "slurm_config.json"
    with config_path.open("w") as f:
        json.dump(slurm_config, f)

    CPUS_PER_TASK = 1
    CPUS_PER_TASK_OVERRIDE = 2
    MEM = 1
    MEM_OVERRIDE = "1G"
    MEM_OVERRIDE_MB = 1000
    CUSTOM_GRES = "my-custom-gres"

    # Create Task
    meta = dict(
        cpus_per_task=CPUS_PER_TASK,
        mem=MEM,
        needs_gpu=False,
    )
    mytask = MockTask(
        name="my-beautiful-task",
        command="python something.py",
        meta=meta,
    )

    # Create WorkflowTask
    meta = dict(
        cpus_per_task=CPUS_PER_TASK_OVERRIDE,
        mem=MEM_OVERRIDE,
        needs_gpu=True,
        gres=CUSTOM_GRES,
    )
    mywftask = MockWorkflowTask(
        task=mytask,
        arguments=dict(message="test"),
        order=0,
        meta=meta,
    )
    debug(mywftask)
    debug(mywftask.overridden_meta)

    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata={"some": "metadata"},
    )
    slurm_config = set_slurm_config(
        wftask=mywftask,
        task_pars=task_pars,
        workflow_dir=(tmp_path / "server"),
        workflow_dir_user=(tmp_path / "user"),
        config_path=config_path,
    )["slurm_options"]
    debug(slurm_config)

    cpus_per_task = slurm_config.cpus_per_task
    mem_per_task_MB = slurm_config.mem_per_task_MB
    partition = slurm_config.partition
    debug(cpus_per_task)
    debug(mem_per_task_MB)
    debug(partition)
    assert cpus_per_task == CPUS_PER_TASK_OVERRIDE
    assert mem_per_task_MB == MEM_OVERRIDE_MB
    assert slurm_config.gres == CUSTOM_GRES
    assert partition == "gpu"
