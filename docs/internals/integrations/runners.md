# Job runner

## Configuration

The runner configuration is defined in the `jobs_runner_config` property of a computational `Resource`.

For a resource of type `local`, this configuration includes a single (optional) parameter, namely the integer variable `parallel_tasks_per_job`. The typical intended use case is that setting `parallel_tasks_per_job` to a small number (e.g. `1`) will limit parallelism when executing tasks requiring a large amount of resources (e.g. memory).

::: fractal_server.runner.config._local.JobRunnerConfigLocal

::: fractal_server.runner.config._slurm.JobRunnerConfigSLURM
