# Task collection

This section is not in-place yet. Meanwhile, you can look at

* https://fractal-analytics-platform.github.io/tasks_spec/
* https://fractal-analytics-platform.github.io/fractal-task-tools/usage/

## Apptainer

Apptainer task collection builds a SIF image from an uploaded pixi-based package, and then runs its tasks inside that image. It is only supported for **SLURM/SSH** resources.

Checklist for enabling it on a cluster:

1. **Resource configuration.** Set `tasks_apptainer_config` in the resource:
    ```json
    "tasks_apptainer_config": {
        "pixi_version": "0.84.2",
        "apptainer_bin": "/some/path/bin/apptainer",
        "SLURM_CONFIG": {
            "partition": "main",
            "cpus": 2,
            "mem": "32G",
            "time": "02:00:00",
            "preamble": []
        }
    }
    ```
    where `pixi_version` is the tag of the base image, `apptainer_bin` is the command used to invoke `apptainer` (either `apptainer` or an absolute path), and `SLURM_CONFIG` is the (required) configuration of the build job.

2. **Job configuration.** Apptainer only mounts some host folders into the container by default (typically `$HOME`, `/tmp` and the current folder), while tasks need to access the job folders and the image data (e.g. the Zarr folders). Any other required folder must be added to `APPTAINER_BINDPATH`, through the `extra_lines` of the resource `jobs_runner_config`, e.g.:
    ```json
    "jobs_runner_config": {
        "default_slurm_config": {
            "extra_lines": ["export APPTAINER_BINDPATH=/shares"]
        },
        "gpu_slurm_config": {
            "extra_lines": ["export APPTAINER_BINDPATH=/shares"]
        }
    }
    ```
    Multiple folders are comma-separated (e.g. `/shares,/scratch`).
