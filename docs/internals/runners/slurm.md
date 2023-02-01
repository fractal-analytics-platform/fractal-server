# SLURM backend

In-progress (for the moment, refer to the
[slurm](../../../reference/fractal_server/app/runner/_slurm) module).

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
