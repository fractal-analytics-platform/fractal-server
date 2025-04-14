from fractal_server.app.runner.executors.slurm_common._slurm_config import (
    SlurmConfig,
)


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
        tasks_per_job=1,
        parallel_tasks_per_job=1,
    )
