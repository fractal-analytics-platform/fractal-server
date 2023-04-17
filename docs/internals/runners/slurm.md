# SLURM backend

(refer to the [slurm](../../../reference/fractal_server/app/runner/_slurm)
module for more details)

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

### Exporting environment variables

The `fractal-server` admin may need to set some global variables that need to
be included in all SLURM submission scripts; this can be achieved via the
`extra_lines` field in the SLURM configuration file, for instance as in
```JSON
{
  "default_slurm_config": {
    "partition": "main",
    "extra_lines": [
      "export SOMEVARIABLE=123",
      "export ANOTHERVARIABLE=ABC"
    ]
  }
}
```

There exists another use case where the value of a variable depends on the user
who runs a certain task. A relevant example is that user A (who will run the
task via SLURM) needs to define the cache-directory paths for some libraries
they use (and those must be paths where user A can write).  This use case is
also supported in the specs of `fractal-server` [SLURM configuration
file](../../../reference/fractal_server/app/runner/_slurm/_slurm_config/#fractal_server.app.runner._slurm._slurm_config.SlurmConfigFile):
If this file includes a block like
```JSON
{
  ...
  "user_local_exports": {
    "LIBRARY_1_CACHE_DIR": "somewhere/library_1",
    "LIBRARY_2_FILE": "somewhere/else/library_2.json"
  }
}
```
then the SLURM submission script will include the lines
```bash
...
export LIBRARY_1_CACHE_DIR=/my/cache/somewhere/library_1
export LIBRARY_2_FILE=/my/cache/somewhere/else/library_2.json
...
```
Note that all paths in the values of `user_local_exports` are interpreted as
relative to a base directory which is user-specific (for instance `/my/cache/`,
in the example above), and which is defined in the `User.cache_dir` attribute.
Also note that in this case `fractal-server` only compiles the configuration
options into lines of the SLURM submission script, without performing any check
on the validity of the given paths.

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
