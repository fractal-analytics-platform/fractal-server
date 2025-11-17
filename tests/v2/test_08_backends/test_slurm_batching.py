from fractal_server.runner.executors.slurm_common._batching import heuristics


def test_heuristics_many_tasks():
    """
    Test a specific case of slurm-job batching.
    """

    NUM_TASKS = 10000
    tasks_per_job, parallel_tasks_per_job = heuristics(
        tot_tasks=NUM_TASKS,
        cpus_per_task=1,
        mem_per_task=100,
        target_cpus_per_job=1,
        max_cpus_per_job=1,
        target_mem_per_job=100,
        max_mem_per_job=100,
        max_num_jobs=1,
        target_num_jobs=10,
    )
    assert tasks_per_job == NUM_TASKS
    assert parallel_tasks_per_job == 1
