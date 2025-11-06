**WORK IN PROGRESS**

## Preliminary checks (with fractal-server 2.16)

These checks should be performed with a working 2.16 Fractal instance, _before_ starting the upgrade procedure.

1. In the user list, identify all users who are actually meant to use this Fractal instance, and mark them as both "active" and "verified".
   * The automated data-migration script described below will only consider users who are both active and verified.
   * Settings for other users won't be modified, and they won't be able to log into Fractal without a manual admin intervention.
2. For all active&verified users, make sure that their `project_dir` is set. This property becomes required in the new version.

## Upgrade procedure

3. Make sure that `.fractal_server.env` includes the `FRACTAL_SLURM_WORKER_PYTHON` variable. If this is not set, add it and set it to the absolute path of the Python interpreter which runs `fractal-server`.
4. Make a copy of a working `.fractal-server.env` file and name it `.fractal-server.env.old`.
5. Make a backup of the current database with `pg_dump` - see e.g. https://fractal-analytics-platform.github.io/fractal-server/internals/database_interface/#backup-and-restore.
6. Stop the fractal-server running process (e.g. via `systemctl stop fractal-server`).
7. Edit `.fractal_server.env` to align with the new version. List of changes:
    * (...)
8. Verify that the following files are available in the current directory:
   * `.fractal_server.env.old`
   * `.fractal_server.env`
   * The JSON file with the SLURM configuration (as defined in the `FRACTAL_SLURM_CONFIG_FILE` variable of `.fractal_server.env.old`).
   * The JSON file with the pixi configuration, if defined in the `FRACTAL_PIXI_CONFIG_FILE` variable of `.fractal_server.env.old`.
9. Replace the current `fractal-server` version with 2.17.0 (e.g. via `pip install fractal-server==2.17.0`).
10. Run the schema-migration command `fractalctl set-db`.
11. Run the data-migration command `fractalctl update-db-data`.
12. Restart the fractal-server process (e.g. via `systemctl start fractal-server`).

## Post-upgrade cleanup

* Upgrade `fractal-web` to version 1.21 and restart it.
* Review the names of resources/profiles.
* Verify that log-in still works.
* Review the association between users and profiles.
* Verify that job execution works as expected.
* Verify that task collection works as expected.
* Verify that the OME-Zarr viewer works as expected (if configured).
