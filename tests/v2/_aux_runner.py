from pathlib import Path

from fractal_server.app.runner.executors.slurm._slurm_config import SlurmConfig
from fractal_server.app.runner.task_files import TaskFiles


def get_default_task_files(
    *, workflow_dir_local: Path, workflow_dir_remote: Path
) -> TaskFiles:
    """
    This will be called when self.submit or self.map are called from
    outside fractal-server, and then lack some optional arguments.
    """
    task_files = TaskFiles(
        workflow_dir_local=workflow_dir_local,
        workflow_dir_remote=workflow_dir_remote,
        task_order=None,
        task_name="name",
    )
    return task_files


def get_default_slurm_config():
    """
    Return a default `SlurmConfig` configuration object
    """
    return SlurmConfig(
        partition="main",
        cpus_per_task=1,
        mem_per_task_MB=100,
        target_cpus_per_job=1,
        max_cpus_per_job=2,
        target_mem_per_job=100,
        max_mem_per_job=500,
        target_num_jobs=2,
        max_num_jobs=4,
    )
