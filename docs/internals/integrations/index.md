# Computational integrations

On top of exposing a web API, a Fractal instance must be integrated to at least one computational resource which is used mainly for two goals:
* Setting up Python environments for task groups - see [more details](./tasks.md).
* Executing scientific tasks - see [more details](./runners.md).

The configuration variable `FRACTAL_RUNNER_BACKEND` determines which one of the three following modality is in-place:

1. In a **local** instance, every computational operation (setting up task environments and executing tasks) is run by the machine user who is running `fractal-server`, and the instance typically only has a single user.

2. A **SLURM/sudo** instance must have access to a SLURM cluster with some additional assumptions - notably:
    * The user that runs `fractal-server` also needs sufficient permissions to impersonate other users for running jobs (e.g. via `sudo -u some-user sbatch /some/submission-script.sh`);
    * There must be a shared filesystem which both the user running `fractal-server` and other users have access to.
3. A **SLURM/SSH** instance must have access to a SLURM cluster through SSH, by impersonating one or several service users.


## Resources and profiles

The specific configuration for each computational resource is defined in the `Resource` database table, with the following creation schemas:

* [Local resource](../../reference/app/schemas/v2/resource/#fractal_server.app.schemas.v2.resource.ValidResourceLocal)
* [SLURM/sudo resource](../../reference/app/schemas/v2/resource/#fractal_server.app.schemas.v2.resource.ValidResourceSlurmSudo)
* [SLURM/SSH resource](../../reference/app/schemas/v2/resource/#fractal_server.app.schemas.v2.resource.ValidResourceSlurmSSH)

For each resource, there may be one or many computational _profiles_, with the following creation schemas:

* [Local profile](../../reference/app/schemas/v2/resource/#fractal_server.app.schemas.v2.profile.ValidProfileLocal)
* [SLURM/sudo profile](../../reference/app/schemas/v2/resource/#fractal_server.app.schemas.v2.profile.ValidProfileeSlurmSudo)
* [SLURM/SSH profile](../../reference/app/schemas/v2/resource/#fractal_server.app.schemas.v2.profile.ValidProfileSlurmSSH)


Here are some minimal examples of how to configure resources and profiles in the three different cases:

--8<-- "docs/assets/resource_and_profile/snippet.md"
