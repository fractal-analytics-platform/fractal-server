## Resource examples

=== "Local"

    ```json
    {
        "type": "local",
        "name": "Local resource",
        "jobs_local_dir": "/somewhere/jobs",
        "jobs_runner_config": {
            "parallel_tasks_per_job": 1
        },
        "jobs_poll_interval": 0,
        "tasks_local_dir": "/somewhere/tasks",
        "tasks_python_config": {
            "default_version": "3.12",
            "versions": {
                "3.12": "/some-venv/bin/python"
            }
        },
        "tasks_pixi_config": {},
        "tasks_pip_cache_dir": null
    }
    ```

=== "SLURM/sudo"

    ```json
    {
        "type": "slurm_sudo",
        "name": "SLURM cluster",
        "jobs_local_dir": "/somewhere/local-jobs",
        "jobs_runner_config": {
            "default_slurm_config": {
                "partition": "partition-name",
                "cpus_per_task": 1,
                "mem": "100M"
            },
            "gpu_slurm_config": {
                "partition": "gpu",
                "extra_lines": [
                    "#SBATCH --gres=gpu:v100:1"
                ]
            },
            "user_local_exports": {
                "CELLPOSE_LOCAL_MODELS_PATH": "CELLPOSE_LOCAL_MODELS_PATH",
                "NAPARI_CONFIG": "napari_config.json"
            },
            "batching_config": {
                "target_cpus_per_job": 1,
                "max_cpus_per_job": 1,
                "target_mem_per_job": 200,
                "max_mem_per_job": 500,
                "target_num_jobs": 2,
                "max_num_jobs": 4
            }
        },
        "jobs_slurm_python_worker": "/some/venv/bin/python3.12",
        "jobs_poll_interval": 10,
        "tasks_local_dir": "/somewhere/local-tasks",
        "tasks_python_config": {
            "default_version": "3.12",
            "versions": {
                "3.11": "/some/venv/bin/python3.11",
                "3.12": "/some/venv/bin/python3.12"
            }
        },
        "tasks_pixi_config": {},
        "tasks_pip_cache_dir": null
    }
    ```

=== "SLURM/SSH"

    ```json
    {
        "type": "slurm_ssh",
        "name": "Remote SLURM cluster",
        "host": "slurm-cluster.example.org",
        "jobs_local_dir": "/somewhere/local-jobs",
        "jobs_runner_config": {
            "default_slurm_config": {
                "partition": "partition-name",
                "cpus_per_task": 1,
                "mem": "100M"
            },
            "gpu_slurm_config": {
                "partition": "gpu",
                "extra_lines": [
                    "#SBATCH --gres=gpu:v100:1"
                ]
            },
            "user_local_exports": {
                "CELLPOSE_LOCAL_MODELS_PATH": "CELLPOSE_LOCAL_MODELS_PATH",
                "NAPARI_CONFIG": "napari_config.json"
            },
            "batching_config": {
                "target_cpus_per_job": 1,
                "max_cpus_per_job": 1,
                "target_mem_per_job": 200,
                "max_mem_per_job": 500,
                "target_num_jobs": 2,
                "max_num_jobs": 4
            }
        },
        "jobs_slurm_python_worker": "/some/venv/bin/python3.12",
        "jobs_poll_interval": 10,
        "tasks_local_dir": "/somewhere/local-tasks",
        "tasks_python_config": {
            "default_version": "3.12",
            "versions": {
                "3.11": "/some/venv/bin/python3.11",
                "3.12": "/some/venv/bin/python3.12"
            }
        },
        "tasks_pixi_config": {},
        "tasks_pip_cache_dir": null
    }
    ```

## Profile examples

=== "Local"

    ```json
    {
        "name": "Local profile",
        "resource_type": "local"
    }
    ```

=== "SLURM/sudo"

    ```json
    {
        "name": "SLURM/sudo profile",
        "resource_type": "slurm_sudo",
        "username": "slurm-username"
    }
    ```

=== "SLURM/SSH"

    ```json
    {
        "name": "SLURM/SSH profile",
        "resource_type": "slurm_ssh",
        "username": "slurm-username",
        "ssh_key_path": "/somewhere/private.key",
        "jobs_remote_dir": "/somewhere/jobs",
        "tasks_remote_dir": "/somewhere/tasks"
    }
    ```
