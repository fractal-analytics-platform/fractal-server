# Local backend

(refer to the [local](../../../reference/fractal_server/app/runner/_local) module for more details)

## Configuration

The logic for setting up the local-backend configuration of a given `WorkflowTask` is
implemented in the
[local.\_local_config](../../../reference/fractal_server/app/runner/_local/_local_config)
submodule.

For the moment, this configuration includes a single (optional) parameter,
namely the ingeger variable `parallel_tasks_per_job`. This parameter is related
to tasks that needs to be run in parallel over several inputs: When
`parallel_tasks_per_job` is set, it will represent the maximum number of tasks
that the backend will run at the same time.

The typical intended use case is that setting `parallel_tasks_per_job` to a
small number (e.g. `1`) will limit parallelism when executing tasks requiring a
large amount of resources (e.g. memory).


The different sources for `parallel_tasks_per_job` are:

1. If the `WorkflowTask.meta` field has a `parallel_tasks_per_job` key, the
   corresponding value takes highest priority;
2. Next priority goes to a `parallel_tasks_per_job` entry in
   `WorkflowTask.task.meta`;
3. Next priority goes to the configuration in `FRACTAL_LOCAL_CONFIG_FILE`, a
   JSON file that may contain a definition of a
   [`LocalBackendConfig`](../../../reference/fractal_server/app/runner/_local/_local_config/#fractal_server.app.runner._local._local_config.LocalBackendConfig)
   object like
```JSON
{
  "parallel_tasks_per_job": 1
}
```
4. Lowest-priority (that is, the default) is to set
   `parallel_tasks_per_job=None`, which corresponds to _not_ limiting
    parallelism at all.
