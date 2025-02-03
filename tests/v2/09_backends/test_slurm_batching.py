from devtools import debug

from fractal_server.app.runner.executors.slurm._batching import heuristics


def test_heuristics():
    res = heuristics(
        tot_tasks=10000,
        cpus_per_task=1,
        mem_per_task=1,
        target_cpus_per_job=1,
        max_cpus_per_job=10,
        target_mem_per_job=1,
        max_mem_per_job=10,
        target_num_jobs=100,
        max_num_jobs=1,
    )
    debug(res)
