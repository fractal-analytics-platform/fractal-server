# SLURM runners

## Configuration

### Environment variables

The `fractal-server` admin may need to set some global variables that should be included in all SLURM submission scripts. This can be achieved via the `extra_lines` SLURM-runner configuration property, for instance as in
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

There exists another use case where the value of a variable depends on the user who runs a certain task. A relevant example is that user A (who will run the task via SLURM) needs to define the cache-directory paths for some libraries they use (and those must be paths where user A can write). This use case is supported through the `user_local_exports` SLURM-runner configuration property. If this is set as in
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
Note that all paths in the values of `user_local_exports` are interpreted as relative to a base directory which is user-specific (e.g. the base cache directory for a user with `project_dir="/my/project_dir"` is `/my/project_dir/.fractal_cache`). Also note that in this case `fractal-server` only compiles the configuration options into lines of the SLURM submission script, without performing any check on the validity of the given paths.

### Task batching

The SLURM backend in `fractal-server` may combine multiple tasks in the same SLURM job (AKA batching), in order to reduce the total number of SLURM jobs that are submitted. This is especially relevant for SLURM clusters with constraints on the number of jobs that a user is allowed to submit over a certain timespan.

The logic for handling the batching parameters (that is, how many tasks can be combined in the same SLURM job, and how many of them can run in parallel) is implemented in [this configuration block](../../reference/runner/config/_slurm.md/#fractal_server.runner.config._slurm.BatchingConfigSet) and in [this submodule](../../reference/runner/executors/slurm_common/_batching.md).


## User impersonation

### `sudo`-based impersonation

The user who runs `fractal-server` must have sufficient privileges for running some commands via `sudo -u` to impersonate other users of the SLURM cluster without any password. The required commands include `sbatch`, `scancel`, `cat`, `ls` and `mkdir`. An example of how to achieve this is to add this block to the `sudoers` file:
```
Runas_Alias FRACTAL_IMPERSONATE_USERS = fractal, user1, user2, user3
Cmnd_Alias FRACTAL_CMD = /usr/bin/sbatch, /usr/bin/scancel, /usr/bin/cat, /usr/bin/ls, /usr/bin/mkdir
fractal ALL=(FRACTAL_IMPERSONATE_USERS) NOPASSWD:FRACTAL_CMD
```
where `fractal` is the user running `fractal-server`, and `{user1,user2,user3}` are the users who can be impersonated. Note that one could also grant `fractal` the option of impersonating a whole UNIX group, instead of listing users one by one.

The user's `Profile` includes the `username` to be impersonated.

### SSH-based impersonation

In this scenario, one or many service users exist on the SLURM cluster and the `fractal-server` SLURM/SSH runner impersonates these service users when connecting to the cluster via SSH for handling jobs. Each Fractal user's `Profile` includes the `username` of the service user to be impersonated, and the path to the corresponding SSH private key.
