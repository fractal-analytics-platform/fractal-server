# Working directories

## Local backend: A single working directory per job

When `fractal-server` executes a task, it has to read/write several
execution-related files (note: these are files used internally by
`fractal-server`, and not the actual scientific data being processed). These
files are stored in a working directory which is unique for each workflow
execution, with a name like `workflow_000001_job_000001` (and within the parent
directory specified in
[`FRACTAL_RUNNER_WORKING_BASE_DIR`](../../../configuration/#fractal_server.config.Settings.FRACTAL_RUNNER_WORKING_BASE_DIR)).
All files in this folder are owned by the user running `fractal-server`, and
the permission set of each folder like `workflow_000001_job_000001` is set to
755.  Note that the folder is created as part of the
[submit_workflow](../../../reference/fractal_server/app/runner/#fractal_server.app.runner.submit_workflow)
function.

## SLURM backend: Server-side and user-side working directories

When using the SLURM backend, parts of the execution-related files are written
by the user who submitted the workflow for execution, [who is impersonated
through the `sudo -u` command](../slurm/#user-impersonation).
Such user has no access to the server-side working directory (the one in
[`FRACTAL_RUNNER_WORKING_BASE_DIR`](../../../configuration/#fractal_server.config.Settings.FRACTAL_RUNNER_WORKING_BASE_DIR)),
and therefore uses a local (i.e. user-side) working directory. By default, as a
parent directory we use the `cache_dir` attribute of the current `User`.
Since the user running `fractal-server` may not have access to this user-side
directory, several operations are run through `sudo -u` (e.g. checking whether
a file exists, or copying its content).
