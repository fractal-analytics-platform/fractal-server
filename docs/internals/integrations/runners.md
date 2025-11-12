# Job runners

The runner is the `fractal-server` components that executes a job (based on a certain workflow and dataset) on a computational resource.

## Configuration

The runner configuration is defined in the `jobs_runner_config` property of a computational resource. The configuration schemas reported below apply to a `local` resource (see `JobRunnerConfigLocal`) and to a `slurm_sudo/slurm_ssh` resource (see `JobRunnerConfigSLURM`). Some more specific details of the SLURM configurations are described at  [advanced SLURM configuration](./_advanced_slurm_config.md).

### ::: fractal_server.runner.config._local.JobRunnerConfigLocal
        options:
            show_root_heading: true
            show_root_toc_entry: false


### ::: fractal_server.runner.config._slurm.JobRunnerConfigSLURM
        options:
            show_root_heading: true
            show_root_toc_entry: false

## Runners

The three runner implementations (for the local, SLURM/sudo and SLURM/SSH cases) are constructed based on the following class hierarchy:

* [`BaseRunner`](../../reference/runner/executors/base_runner.md/#fractal_server.runner.executors.base_runner.BaseRunner) is the base class for all runners, which notably includes the `submit` and `multisubmit` methods (to be overridden in child classes).
    * [`LocalRunner`](../../reference/runner/executors/local/runner.md/#fractal_server.runner.executors.local.runner.LocalRunner) is the runner implementation for a `local` computational resource.
    * [`BaseSlurmRunner`](../../reference/runner/executors/slurm_common/base_slurm_runner.md/#fractal_server.runner.executors.slurm_common.base_slurm_runner.BaseSlurmRunner) inherits from `BaseRunner` and adds the common part of SLURM runners:
        * [`SudoSlurmRunner`](../../reference/runner/executors/slurm_sudo/runner.md/#fractal_server.runner.executors.slurm_sudo.runner.SudoSlurmRunner) is the runner implementation for a `slurm_sudo` resource.
        * [`SlurmSSHRunner`](../../reference/runner/executors/slurm_ssh/runner.md/#fractal_server.runner.executors.slurm_ssh.runner.SlurmSSHRunner) is the runner implementation for a `slurm_ssh` resource.
