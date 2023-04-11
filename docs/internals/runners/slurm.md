# SLURM backend

In-progress (for the moment, refer to the
[slurm](../../../reference/fractal_server/app/runner/_slurm) module).

## SLURM configuration

The logic for setting up the SLURM configuration of a given `WorkflowTask` is
implemented in the
[slurm.\_slurm_config](../../../reference/fractal_server/app/runner/_slurm/_slurm_config)
submodule.

The different sources for SLURM configuration options (like `partition`, `cpus_per_task`, ...) are:

1. All attributes that are explicitly set in the `WorkflowTask.meta` dictionary
   attribute take highest priority;
2. Next priority goes to all attributes that are explicitly set in the
   `WorkflowTask.task.meta` dictionary attribute;
3. Lowest-priority (that is default) values come from the configuration in
   `FRACTAL_SLURM_CONFIG_FILE`. This JSON file follows [these
specifications](../../../reference/fractal_server/app/runner/_slurm/_slurm_config/#fractal_server.app.runner._slurm._slurm_config.SlurmConfigFile).

### Example

The configuration file could be the one defined [here](../../../reference/fractal_server/app/runner/_slurm/_slurm_config/#fractal_server.app.runner._slurm._slurm_config.SlurmConfigFile), while a certain `WorkflowTask` could have
```python
workflow_task.meta = {"cpus_per_task": 3}
workflow_task.task.meta = {"cpus_per_task": 2, "mem": "10G"}
```
In this case, the SLURM configuration for this `WorkflowTask` will correspond to
```
partition=main
cpus_per_task=3
mem=10G
```

## SLURM batching

The SLURM backend in `fractal-server` may combine multiple tasks in the same
SLURM job (AKA batching), in order to reduce the total number of SLURM jobs
that are submitted. This is especially relevant for clusters with constraints
on the number of jobs that a user is allowed to submit over a certain timespan.

The logic for handling the batching parameters (that is, how many tasks can be
combined in the same SLURM job, and how many of them can run in parallel) is
implemented in the
[slurm.\_batching](../../../reference/fractal_server/app/runner/_slurm/_batching)
submodule, and especially in its
[`heuristics`](../../../reference/fractal_server/app/runner/_slurm/_batching/#fractal_server.app.runner._slurm._batching.heuristics)
function.


## User impersonation

The user who runs `fractal-server` must have sufficient priviliges for running
some commands via `sudo -u` to impersonate other users of the SLURM cluster
without any password. The required commands include `sbatch`, `scancel`, `cat`,
`ls` and `mkdir`. An example of how to achieve this is to add this block to the
`sudoers` file:
```
Runas_Alias FRACTAL_IMPERSONATE_USERS = fractal, user1, user2, user3
Cmnd_Alias FRACTAL_CMD = /usr/bin/sbatch, /usr/bin/scancel, /usr/bin/cat, /usr/bin/ls, /usr/bin/mkdir
fractal ALL=(FRACTAL_IMPERSONATE_USERS) NOPASSWD:FRACTAL_CMD
```
where `fractal` is the user running `fractal-server`, and `{user1,user2,user3}`
are the users who can be impersonated. Note that one could also grant `fractal`
the option of impersonating a whole UNIX group, instead of listing users one by one.
