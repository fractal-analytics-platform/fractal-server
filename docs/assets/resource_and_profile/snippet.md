## Resource example

=== "Local"

    ```json
    {
        "type": "local",
        "name": "local resource",
        "jobs_local_dir": "/somewhere/jobs",
        "jobs_runner_config": {
            "parallel_tasks_per_job": 1
        },
        "jobs_poll_interval": 0,
        "tasks_local_dir": "/somewhere/tasks",
        "tasks_python_config": {
            "default_version": "3.12",
            "versions": {
                "3.12": "/over-the-rainbow/bin/python"
            }
        },
        "tasks_pixi_config": {},
        "host": null,
        "jobs_slurm_python_worker": null,
        "tasks_pip_cache_dir": null
    }
    ```

=== "SLURM sudo"

    ```json
    {
        "type": "slurm_sudo",
        "name": "SLURM cluster A",
        "jobs_local_dir": "/somewhere/local-jobs",
        "jobs_runner_config": {
            "default_slurm_config": {
                "partition": "main",
                "cpus_per_task": 1,
                "mem": "100M"
            },
            "gpu_slurm_config": {},
            "batching_config": {
                "target_cpus_per_job": 1,
                "max_cpus_per_job": 1,
                "target_mem_per_job": 200,
                "max_mem_per_job": 500,
                "target_num_jobs": 2,
                "max_num_jobs": 4
            }
        },
        "jobs_slurm_python_worker": "/.venv3.12/bin/python3.12",
        "jobs_poll_interval": 0,
        "tasks_local_dir": "/somewhere/local-tasks",
        "tasks_python_config": {
            "default_version": "3.12",
            "versions": {
                "3.10": "/.venv3.10/bin/python3.10",
                "3.11": "/.venv3.11/bin/python3.11",
                "3.12": "/.venv3.12/bin/python3.12"
            }
        },
        "tasks_pixi_config": {},
        "host": null,
        "tasks_pip_cache_dir": null
    }
    ```

=== "SLURM ssh"

    ```json
    {
        "type": "slurm_ssh",
        "name": "SLURM cluster A",
        "host": "localhost",
        "jobs_local_dir": "/somewhere/local-jobs",
        "jobs_runner_config": {
            "default_slurm_config": {
                "partition": "main",
                "cpus_per_task": 1,
                "mem": "100M"
            },
            "gpu_slurm_config": {},
            "batching_config": {
                "target_cpus_per_job": 1,
                "max_cpus_per_job": 1,
                "target_mem_per_job": 200,
                "max_mem_per_job": 500,
                "target_num_jobs": 2,
                "max_num_jobs": 4
            }
        },
        "jobs_slurm_python_worker": "/.venv3.12/bin/python3.12",
        "jobs_poll_interval": 0,
        "tasks_local_dir": "/somewhere/local-tasks",
        "tasks_python_config": {
            "default_version": "3.12",
            "versions": {
                "3.10": "/.venv3.10/bin/python3.10",
                "3.11": "/.venv3.11/bin/python3.11",
                "3.12": "/.venv3.12/bin/python3.12"
            }
        },
        "tasks_pixi_config": {},
        "tasks_pip_cache_dir": null
    }
    ```

## Profile example

=== "Local"

    ```json
    {
        "resource_type": "local",
        "name": "profile local",
        "username": null,
        "ssh_key_path": null,
        "jobs_remote_dir": null,
        "tasks_remote_dir": null
    }
    ```

=== "SLURM sudo"

    ```json
    {
        "resource_type": "slurm_sudo",
        "name": "profile sudo",
        "username": "test01",
        "ssh_key_path": null,
        "jobs_remote_dir": null,
        "tasks_remote_dir": null
    }
    ```

=== "SLURM ssh"

    ```json
    {
        "resource_type": "slurm_ssh",
        "name": "profile ssh",
        "username": "test01",
        "ssh_key_path": "/fake/key",
        "jobs_remote_dir": "/fake/jobs",
        "tasks_remote_dir": "/fake/tasks"
    }
    ```
