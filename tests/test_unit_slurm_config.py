from devtools import debug

from .fixtures_tasks import MockTask
from .fixtures_tasks import MockWorkflowTask
from fractal_server.app.runner._grouped_slurm._slurm_config import (
    set_slurm_config,
)  # noqa
from fractal_server.app.runner.common import TaskParameters


def test_set_slurm_config(tmp_path):

    meta = dict(cpus_per_task=1, mem=1, needs_gpu=False)
    mytask = MockTask(
        name="my-beautiful-task",
        command="python something.py",
        meta=meta,
    )
    mywftask = MockWorkflowTask(
        task=mytask,
        arguments=dict(message="test"),
        order=0,
    )
    debug(mywftask)
    debug(mywftask.overridden_meta)

    task_pars = TaskParameters(
        input_paths=[tmp_path],
        output_path=tmp_path,
        metadata={"some": "metadata"},
    )
    out = set_slurm_config(
        wftask=mywftask,
        task_pars=task_pars,
        workflow_dir=(tmp_path / "server"),
        workflow_dir_user=(tmp_path / "user"),
    )
    debug(out)
