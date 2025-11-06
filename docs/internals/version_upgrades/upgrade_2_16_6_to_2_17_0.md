# Upgrade from 2.16 to 2.17.0

Because of the broad scope of the 2.17.0 version, this page describes the upgrade procedure in detail.
Notable changes in this version include:

* New concepts of computational resources and profiles.
* Updates of the application settings.
* Users cannot self-register through OAuth any more.
* A user must meet more conditions in order to access the API (namely being marked as both active and verified, and being associated to a computational profile).

> Note: Automatic data migration for version 2.17.0 is only supported for Fractal instances attached on a SLURM cluster, and not for `local` instances.

## Preliminary checks (with fractal-server 2.16)

These checks should be performed on a working 2.16 Fractal instance, _before_ starting the upgrade procedure.

1. In the user list, identify all users who are actually meant to use this Fractal instance, and mark them as both "active" and "verified".
    * The automated data-migration script described below will only look for users who are both active and verified.
    * Settings for other users won't be modified, and they won't be able to use Fractal without a manual admin intervention.
2. For all active&verified users, make sure that their `project_dir` is set. The data-migration script will fail if it is not set for some active&verified user - because `project_dir` becomes a required user property.

## Upgrade procedure

1. Make a copy of the current `.fractal-server.env` file and name it `.fractal-server.env.old`.
2. Make sure that `.fractal_server.env.old` includes the `FRACTAL_SLURM_WORKER_PYTHON` variable. If this variable is not set, add it and set it to the absolute path of the Python interpreter which runs `fractal-server`.
3. Make a backup of the current database with `pg_dump` (see [example](../../database_interface/#backup-and-restore)).
4. Stop the fractal-server running process (e.g. via `systemctl stop fractal-server`).
5. Edit `.fractal_server.env` to align with the new version. List of changes:
    - Edit the `FRACTAL_RUNNER_BACKEND` value so that it is one of `slurm_sudo` or `slurm_ssh`.
    - Rename `FRACTAL_VIEWER_AUTHORIZATION_SCHEME` into `FRACTAL_DATA_AUTH_SCHEME` - if present.
    - Rename `FRACTAL_VIEWER_BASE_FOLDER` into `FRACTAL_DATA_BASE_FOLDER` - if present.
    - Update OAuth-related variables to comply with [the new expected ones](../../../reference/fractal_server/config/_oauth/#fractal_server.config._oauth.OAuthSettings).
        - Add the `OAUTH_CLIENT_NAME` variable.
        - Remove the client name from the names of all other variables, e.g. as in `OAUTH_XXX_CLIENT_ID --> OAUTH_CLIENT_ID` (if `OAUTH_CLIENT_NAME="XXX"`).
    - Drop all following variables (if set):
        - `FRACTAL_DEFAULT_ADMIN_EMAIL`
        - `FRACTAL_DEFAULT_ADMIN_PASSWORD`
        - `FRACTAL_DEFAULT_ADMIN_USERNAME`
        - `FRACTAL_TASKS_DIR`
        - `FRACTAL_RUNNER_WORKING_BASE_DIR`
        - `FRACTAL_LOCAL_CONFIG_FILE`
        -  `FRACTAL_SLURM_CONFIG_FILE`
        - `FRACTAL_SLURM_WORKER_PYTHON`.
        - `FRACTAL_TASKS_PYTHON_DEFAULT_VERSION`
        - All `FRACTAL_TASKS_PYTHON_3_*` variables
        - `FRACTAL_PIXI_CONFIG_FILE`.
        - `FRACTAL_SLURM_POLL_INTERVAL`.
        - `FRACTAL_PIP_CACHE_DIR`
6. Verify that the following files are available in the current directory:
   * `.fractal_server.env.old`
   * `.fractal_server.env`
   * The JSON file with the SLURM configuration (as defined in the `FRACTAL_SLURM_CONFIG_FILE` variable of `.fractal_server.env.old`).
   * The JSON file with the pixi configuration, if defined in the `FRACTAL_PIXI_CONFIG_FILE` variable of `.fractal_server.env.old`.
7. Replace the current `fractal-server` version with 2.17.0 (e.g. via `pip install fractal-server==2.17.0` - within the appropriate Python environment).
8. Run the database-schema-migration command `fractalctl set-db`.
9. Run the database-data-migration command `fractalctl update-db-data`.
10. Restart the fractal-server process (e.g. via `systemctl start fractal-server`).

## Post-upgrade cleanup

* Upgrade `fractal-web` to version 1.21 and restart it.
* Verify that log-in still works (including via OAuth).
* Review the names of resources/profiles.
* Review the association between users and profiles.
* Verify that job execution works as expected.
* Verify that task collection works as expected.
* Verify that the OME-Zarr viewer works as expected (if configured).
