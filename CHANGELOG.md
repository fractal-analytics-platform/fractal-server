**Note**: Numbers like (\#123) point to closed Pull Requests on the fractal-server repository.


# 1.4.10

> WARNING: Starting from this version, the dependencies for the `slurm` extra
> are required; commands like `pip install fractal-server[slurm,postgres]` must
> be replaced by `pip install fractal-server[postgres]`.

* Dependencies:
    * Make `clusterfutures` and `cloudpickle` required dependencies (\#1255).
    * Remove `slurm` extra from package (\#1255).
* API:
    * Handle invalid history file in `GET /project/{project_id}/dataset/{dataset_id}/status/` (\#1259).
* Runner:
    * Add custom `_jobs_finished` function to check the job status and to avoid squeue errors (\#1266)

# 1.4.9

This release is a follow-up of 1.4.7 and 1.4.8, to mitigate the risk of
job folders becoming very large.

* Runner:
    * Exclude `history` from `TaskParameters` object for parallel tasks, so that it does not end up in input pickle files (\#1247).

# 1.4.8

This release is a follow-up of 1.4.7, to mitigate the risk of job folders
becoming very large.

* Runner:
    * Exclude `metadata["image"]` from `TaskParameters` object for parallel tasks, so that it does not end up in input pickle files (\#1245).
    * Exclude components list from `workflow.log` logs (\#1245).
* Database:
    * Remove spurious logging of `fractal_server.app.db` string (\#1245).

# 1.4.7

This release provides a bugfix (PR 1239) and a workaround (PR 1238) for the
SLURM runner, which became relevant for the use case of processing a large
dataset (300 wells with 25 cycles each).

* Runner:
    * Do not include `metadata["image"]` in JSON file with task arguments (\#1238).
    * Add `FRACTAL_RUNNER_TASKS_INCLUDE_IMAGE` configuration variable, to define exceptions where tasks still require `metadata["image"]` (\#1238).
    * Fix bug in globbing patterns, when copying files from user-side to server-side job folder in SLURM executor (\#1239).
* API:
    * Fix error message for rate limits in apply-workflow endpoint (\#1231).
* Benchmarks:
    * Add more scenarios, as per issue \#1184 (\#1232).

# 1.4.6

* API:
    * Add `GET /admin/job/{job_id}` (\#1230).
    * Handle `FileNotFound` in `GET /project/{project_id}/job/{job_id}/` (\#1230).

# 1.4.5

* Remove CORS middleware (\#1228).
* Testing:
    *  Fix `migrations.yml` GitHub action (\#1225).

# 1.4.4

* API:
    * Add rate limiting to `POST /{project_id}/workflow/{workflow_id}/apply/` (\#1199).
    * Allow users to read the logs of ongoing jobs with `GET /project/{project_id}/job/{job_id}/`, using `show_tmp_logs` query parameter (\#1216).
    * Add `log` query parameter in `GET {/api/v1/job/,/api/v1/{project.id}/job/,/admin/job/}`, to trim response body (\#1218).
    * Add `args_schema` query parameter in `GET /api/v1/task/` to trim response body (\#1218).
    * Add `history` query parameter in `GET {/api/v1/dataset/,/api/v1/project/{project.id}/dataset/}` to trim response body (\#1219).
    * Remove `task_list` from `job.workflow_dump` creation in `/api/v1/{project_id}/workflow/{workflow_id}/apply/`(\#1219)
    * Remove `task_list` from `WorkflowDump` Pydantic schema (\#1219)
* Dependencies:
    * Update fastapi to `^0.109.0` (\#1222).
    * Update gunicorn to `^21.2.0` (\#1222).
    * Update aiosqlite to `^0.19.0` (\#1222).
    * Update uvicorn to `^0.27.0` (\#1222).

# 1.4.3

> **WARNING**:
>
> This update requires running a fix-db script, via `fractalctl update-db-data`.

* API:
    * Improve validation of `UserCreate.slurm_accounts` (\#1162).
    * Add `timestamp_created` to `WorkflowRead`, `WorkflowDump`, `DatasetRead` and `DatasetDump` (\#1152).
    * Make all dumps in `ApplyWorkflowRead` non optional (\#1175).
    * Ensure that timestamps in `Read` schemas are timezone-aware, regardless of `DB_ENGINE` (\#1186).
    * Add timezone-aware timestamp query parameters to all `/admin` endpoints (\#1186).
* API (internal):
    * Change the class method `Workflow.insert_task` into the auxiliary function `_workflow_insert_task` (\#1149).
* Database:
    * Make `WorkflowTask.workflow_id` and `WorfklowTask.task_id` not nullable (\#1137).
    * Add `Workflow.timestamp_created` and `Dataset.timestamp_created` columns (\#1152).
    * Start a new `current.py` fix-db script (\#1152, \#1195).
    * Add to `migrations.yml` a new script (`validate_db_data_with_read_schemas.py`) that validates test-DB data with Read schemas (\#1187).
    * Expose `fix-db` scripts via command-line option `fractalctl update-db-data` (\#1197).
* App (internal):
    * Check in `Settings` that `psycopg2`, `asyngpg` and `cfut`, if required, are installed (\#1167).
    * Split `DB.set_db` into sync/async methods (\#1165).
    * Rename `DB.get_db` into `DB.get_async_db` (\#1183).
    * Normalize names of task packages (\#1188).
* Testing:
    * Update `clean_db_fractal_1.4.1.sql` to `clean_db_fractal_1.4.2.sql`, and change `migrations.yml` target version (\#1152).
    * Reorganise the test directory into subdirectories, named according to the order in which we want the CI to execute them (\#1166).
    * Split the CI into two independent jobs, `Core` and `Runner`, to save time through parallelisation (\#1204).
* Dependencies:
    * Update `python-dotenv` to version 0.21.0 (\#1172).
* Runner:
    * Remove `JobStatusType.RUNNING`, incorporating it into `JobStatusType.SUBMITTED` (\#1179).
* Benchmarks:
    * Add `fractal_client.py` and `populate_script_v2.py` for creating different database status scenarios (\#1178).
    * Add a custom benchmark suite in `api_bench.py`.
    * Remove locust.
* Documentation:
    * Add the minimum set of environment variables required to set the database and start the server (\#1198).

# 1.4.2

> **WARNINGs**:
>
> 1. This update requires running a fix-db script, available at https://raw.githubusercontent.com/fractal-analytics-platform/fractal-server/1.4.2/scripts/fix_db/current.py.
> 2. Starting from this version, non-verified users have limited access to `/api/v1/` endpoints. Before the upgrade, all existing users must be manually set to verified.

* API:
    * Prevent access to `GET/PATCH` task endpoints for non-verified users (\#1114).
    * Prevent access to task-collection and workflow-apply endpoints for non-verified users (\#1099).
    * Make first-admin-user verified (\#1110).
    * Add the automatic setting of `ApplyWorkflow.end_timestamp` when patching `ApplyWorkflow.status` via `PATCH /admin/job/{job_id}` (\#1121).
    * Change `ProjectDump.timestamp_created` type from `datetime` to `str` (\#1120).
    * Change `_DatasetHistoryItem.workflowtask` type into `WorkflowTaskDump` (\#1139).
    * Change status code of stop-job endpoints to 202 (\#1151).
* API (internal):
    * Implement cascade operations explicitly, in `DELETE` endpoints for datasets, workflows and projects (\#1130).
    * Update `GET /project/{project_id}/workflow/{workflow_id}/job/` to avoid using `Workflow.job_list` (\#1130).
    * Remove obsolete sync-database dependency from apply-workflow endpoint (\#1144).
* Database:
    * Add `ApplyWorkflow.project_dump` column (\#1070).
    * Provide more meaningful names to fix-db scripts (\#1107).
    * Add `Project.timestamp_created` column, with timezone-aware default (\#1102, \#1131).
    * Remove `Dataset.list_jobs_input` and `Dataset.list_jobs_output` relationships (\#1130).
    * Remove `Workflow.job_list` (\#1130).
* Runner:
    * In SLURM backend, use `slurm_account` (as received from apply-workflow endpoint) with top priority (\#1145).
    * Forbid setting of SLURM account from `WorkflowTask.meta` or as part of `worker_init` variable (\#1145).
    * Include more info in error message upon `sbatch` failure (\#1142).
    * Replace `sbatch` `--chdir` option with `-D`, to support also slurm versions before 17.11 (\#1159).
* Testing:
    * Extended systematic testing of database models (\#1078).
    * Review `MockCurrentUser` fixture, to handle different kinds of users (\#1099).
    * Remove `persist` from `MockCurrentUser` (\#1098).
    * Update `migrations.yml` GitHub Action to use up-to-date database and also test fix-db script (\#1101).
    * Add more schema-based validation to fix-db current script (\#1107).
    * Update `.dict()` to `.model_dump()` for `SQLModel` objects, to fix some `DeprecationWarnings`(\##1133).
    * Small improvement in schema coverage (\#1125).
    * Add unit test for `security` module (\#1036).
* Dependencies:
    * Update `sqlmodel` to version 0.0.14 (\#1124).
* Benchmarks:
    * Add automatic benchmark system for API's performances (\#1123)
* App (internal):
    * Move `_create_first_user` from `main` to `security` module, and allow it to create multiple regular users (\#1036).

# 1.4.1

* API:
    * Add `GET /admin/job/{job_id}/stop/` and `GET /admin/job/{job_id}/download/` endpoints (\#1059).
    * Use `DatasetDump` and `WorkflowDump` models for "dump" attributes of `ApplyWorkflowRead` (\#1049, \#1082).
    * Add `slurm_accounts` to `User` schemas and add `slurm_account` to `ApplyWorkflow` schemas (\#1067).
    * Prevent providing a `package_version` for task collection from a `.whl` local package (\#1069).
    * Add `DatasetRead.project` and `WorkflowRead.project` attributes (\#1082).
* Database:
    * Make `ApplyWorkflow.workflow_dump` column non-nullable (\#1049).
    * Add `UserOAuth.slurm_accounts` and `ApplyWorkflow.slurm_account` columns (\#1067).
    * Add script for adding `ApplyWorkflow.user_email` (\#1058).
    * Add `Dataset.project` and `Workflow.project` relationships (\#1082).
    * Avoid using `Project` relationships `dataset_list` or `workflow_list` within some `GET` endpoints (\#1082).
    * Fully remove `Project` relationships `dataset_list`, `workflow_list` and `job_list` (\#1091).
* Testing:
    * Only use ubuntu-22.04 in GitHub actions (\#1061).
    * Improve unit testing of database models (\#1082).
* Dependencies:
    * Pin `bcrypt` to 4.0.1 to avoid warning in passlib (\#1060).
* Runner:
    *  Set SLURM-job working directory to `job.working_dir_user` through `--chdir` option (\#1064).

# 1.4.0

* API:
    * Major endpoint changes:
        * Add trailing slash to _all_ endpoints' paths (\#1003).
        * Add new admin-area endpoints restricted to superusers at `/admin` (\#947, \#1009, \#1032).
        * Add new `GET` endpoints `api/v1/job/` and `api/v1/project/{project_id}/workflow/{workflow_id}/job/` (\#969, \#1003).
        * Add new `GET` endpoints `api/v1/dataset/` and `api/v1/workflow/` (\#988, \#1003).
        * Add new `GET` endpoint `api/v1/project/{project_id}/dataset/` (\#993).
        * Add `PATCH /admin/job/{job_id}/` endpoint (\#1030, \#1053).
        * Move `GET /auth/whoami/` to `GET /auth/current-user/` (\#1013).
        * Move `PATCH /auth/users/me/` to `PATCH /auth/current-user/` (\#1013, \#1035).
        * Remove `DELETE /auth/users/{id}/` endpoint (\#994).
        * Remove `GET /auth/users/me/` (\#1013).
        * Remove `POST` `/auth/forgot-password/`, `/auth/reset-password/`, `/auth/request-verify-token/`, `/auth/verify/` (\#1033).
        * Move `GET /auth/userlist/` to `GET /auth/users/` (\#1033).
    * New behaviors or responses of existing endpoints:
        * Change response of `/api/v1/project/{project_id}/job/{job_id}/stop/` endpoint to 204 no-content (\#967).
        * Remove `dataset_list` attribute from `ProjectRead`, which affects all `GET` endpoints that return some project (\#993).
        * Make it possible to delete a `Dataset`, `Workflow` or `Project`, even when it is in relationship to an `ApplyWorkflow` - provided that the `ApplyWorkflow` is not pending or running (\#927, \#973).
        * Align `ApplyWorkflowRead` with new `ApplyWorkflow`, which has optional foreign keys `project_id`, `workflow_id`, `input_dataset_id`, and `output_dataset_id` (\#984).
        * Define types for `ApplyWorkflowRead` "dump" attributes (\#990). **WARNING**: reverted with \#999.
    * Internal changes:
        * Move all routes definitions into `fractal_server/app/routes` (\#976).
        * Fix construction of `ApplyWorkflow.workflow_dump`, within apply endpoint (\#968).
        * Fix construction of `ApplyWorkflow` attributes `input_dataset_dump` and `output_dataset_dump`, within apply endpoint (\#990).
        * Remove `asyncio.gather`, in view of SQLAlchemy2 update (\#1004).
* Database:
    * Make foreign-keys of `ApplyWorkflow` (`project_id`, `workflow_id`, `input_dataset_id`, `output_dataset_id`) optional (\#927).
    * Add columns `input_dataset_dump`, `output_dataset_dump` and `user_email` to `ApplyWorkflow` (\#927).
    * Add relations `Dataset.list_jobs_input` and `Dataset.list_jobs_output` (\#927).
    * Make `ApplyWorkflow.start_timestamp` non-nullable (\#927).
    * Remove `"cascade": "all, delete-orphan"` from `Project.job_list` (\#927).
    * Add `Workflow.job_list` relation (\#927).
    * Do not use `Enum`s as column types (e.g. for `ApplyWorkflow.status`), but only for (de-)serialization (\#974).
    * Set `pool_pre_ping` option to `True`, for asyncpg driver (\#1037).
    * Add script for updating DB from 1.4.0 to 1.4.1 (\#1010)
    * Fix missing try/except in sync session (\#1020).
* App:
    * Skip creation of first-superuser when one superuser already exists (\#1006).
* Dependencies:
    * Update sqlalchemy to version `>=2.0.23,<2.1` (\#1044).
    * Update sqlmodel to version 0.0.12 (\#1044).
    * Upgrade asyncpg to version 0.29.0 (\#1036).
* Runner:
    * Refresh DB objects within `submit_workflow` (\#927).
* Testing:
    * Add `await db_engine.dispose()` in `db_create_tables` fixture (\#1047).
    * Set `debug=False` in `event_loop` fixture (\#1044).
    * Improve `test_full_workflow.py` (\#971).
    * Update `pytest-asyncio` to v0.21 (\#1008).
    * Fix CI issue related to event loop and asyncpg (\#1012).
    * Add GitHub Action testing database migrations (\#1010).
    * Use greenlet v3 in `poetry.lock` (\#1044).
* Documentation:
    * Add OAuth2 example endpoints to Web API page (\#1034, \#1038).
* Development:
    * Use poetry 1.7.1 (\#1043).

# 1.3.14 (do not use!)

> **WARNING**: This version introduces a change that is then reverted in 1.4.0,
> namely it sets the `ApplyWorkflow.status` type to `Enum`, when used with
> PostgreSQL. It is recommended to **not** use it, and upgrade to 1.4.0
> directly.

* Make `Dataset.resource_list` an `ordering_list`, ordered by `Resource.id` (\#951).
* Expose `redirect_url` for OAuth clients (\#953).
* Expose JSON Schema for the `ManifestV1` Pydantic model (\#942).
* Improve delete-resource endpoint (\#943).
* Dependencies:
    * Upgrade sqlmodel to 0.0.11 (\#949).
* Testing:
    * Fix bug in local tests with Docker/SLURM (\#948).

# 1.3.13

* Configure sqlite WAL to avoid "database is locked" errors (\#860).
* Dependencies:
    * Add `sqlalchemy[asyncio]` extra, and do not directly require `greenlet` (\#895).
    * Fix `cloudpickle`-version definition in `pyproject.toml` (\#937).
    * Remove obsolete `sqlalchemy_utils` dependency (\#939).
* Testing:
    * Use ubuntu-22 for GitHub CI (\#909).
    * Run GitHub CI both with SQLite and Postgres (\#915).
    * Disable `postgres` service in GitHub action when running tests with SQLite (\#931).
    * Make `test_commands.py` tests stateless, also when running with Postgres (\#917).
* Documentation:
    * Add information about minimal supported SQLite version (\#916).

# 1.3.12

* Project creation:
    * Do not automatically create a dataset upon project creation (\#897).
    * Remove `ProjectCreate.default_dataset_name` attribute (\#897).
* Dataset history:
    * Create a new (**non-nullable**) history column in `Dataset` table (\#898, \#901).
    * Deprecate history handling in `/project/{project_id}/job/{job_id}` endpoint (\#898).
    * Deprecate `HISTORY_LEGACY` (\#898).
* Testing:
    * Remove obsolete fixture `slurm_config` (\#903).

# 1.3.11

This is mainly a bugfix release for the `PermissionError` issue.

* Fix `PermissionError`s in parallel-task metadata aggregation for the SLURM backend (\#893).
* Documentation:
    * Bump `mkdocs-render-swagger-plugin` to 0.1.0 (\#889).
* Testing:
    * Fix `poetry install` command and `poetry` version in GitHub CI (\#889).

# 1.3.10

Warning: updating to this version requires changes to the configuration variable

* Updates to SLURM interface:
    * Remove `sudo`-requiring `ls` calls from `FractalFileWaitThread.check` (\#885);
    * Change default of `FRACTAL_SLURM_POLL_INTERVAL` to 5 seconds (\#885);
    * Rename `FRACTAL_SLURM_OUTPUT_FILE_GRACE_TIME` configuration variables into `FRACTAL_SLURM_ERROR_HANDLING_INTERVAL` (\#885);
    * Remove `FRACTAL_SLURM_KILLWAIT_INTERVAL` variable and corresponding logic (\#885);
    * Remove `_multiple_paths_exist_as_user` helper function (\#885);
    * Review type hints and default values of SLURM-related configuration variables (\#885).
* Dependencies:
    * Update `fastapi` to version `^0.103.0` (\#877);
    * Update `fastapi-users` to version `^12.1.0` (\#877).

# 1.3.9

* Make updated-metadata collection robust for metadiff files consisting of a single `null` value (\#879).
* Automate procedure for publishing package to PyPI (\#881).

# 1.3.8

* Backend runner:
    * Add aggregation logic for parallel-task updated metadata (\#852);
    * Make updated-metadata collection robust for missing files (\#852, \#863).
* Database interface:
* API:
    * Prevent user from bypassing workflow-name constraint via the PATCH endpoint (\#867).
    * Handle error upon task collection, when tasks exist in the database but not on-disk (\#874).
    * Add `_check_project_exists` helper function (\#872).
* Configuration variables:
    * Remove `DEPLOYMENT_TYPE` variable and update `alive` endpoint (\#875);
    * Introduce `Settings.check_db` method, and call it during inline/offline migrations (\#855);
    * Introduce `Settings.check_runner` method (\#875);
    * Fail if `FRACTAL_BACKEND_RUNNER` is `"local"` and `FRACTAL_LOCAL_CONFIG_FILE` is set but missing on-disk (\#875);
    * Clean up `Settings.check` method and improve its coverage (\#875);
* Package, repository, documentation:
    * Change `fractal_server.common` from being a git-submodule to being a regular folder (\#859).
    * Pin documentation dependencies (\#865).
    * Split `app/models/project.py` into two modules for dataset and project (\#871).
    * Revamp documentation on database interface and on the corresponding configuration variables (\#855).


# 1.3.7

* Oauth2-related updates (\#822):
    * Update configuration of OAuth2 clients, to support OIDC/GitHub/Google;
    * Merge `SQLModelBaseOAuthAccount` and `OAuthAccount` models;
    * Update `UserOAuth.oauth_accounts` relationship and fix `list_users` endpoint accordingly;
    * Introduce dummy `UserManager.on_after_login` method;
    * Rename `OAuthClient` into `OAuthClientConfig`;
    * Revamp users-related parts of documentation.

# 1.3.6

* Update `output_dataset.meta` also when workflow execution fails (\#843).
* Improve error message for unknown errors in job execution (\#843).
* Fix log message incorrectly marked as "error" (\#846).

# 1.3.5

* Review structure of dataset history (\#803):
    * Re-define structure for `history` property of `Dataset.meta`;
    * Introduce `"api/v1/project/{project_id}/dataset/{dataset_id}/status/"` endpoint;
    * Introduce `"api/v1/project/{project_id}/dataset/{dataset_id}/export_history/"` endpoint;
    * Move legacy history to `Dataset.meta["HISTORY_LEGACY"]`.
* Make `first_task_index` and `last_task_index` properties of `ApplyWorkflow` required (\#803).
* Add `docs_info` and `docs_link` to Task model (\#814)
* Accept `TaskUpdate.version=None` in task-patch endpoint (\#818).
* Store a copy of the `Workflow` into the optional column `ApplyWorkflow.workflow_dump` at the time of submission (\#804, \#834).
* Prevent execution of multiple jobs with the same output dataset (\#801).
* Transform non-absolute `FRACTAL_TASKS_DIR` into absolute paths, relative to the current working directory (\#825).
* Error handling:
    * Raise an appropriate error if a task command is not executable (\#800).
    * Improve handling of errors raised in `get_slurm_config` (\#800).
* Documentation:
    * Clarify documentation about `SlurmConfig` (\#798).
    * Update documentation configuration and GitHub actions (\#811).
* Tests:
    * Move `tests/test_common.py` into `fractal-common` repository (\#808).
    * Switch to `docker compose` v2 and unpin `pyyaml` version (\#816).

# 1.3.4

* Support execution of a workflow subset (\#784).
* Fix internal server error for invalid `task_id` in `create_workflowtask` endpoint (\#782).
* Improve logging in background task collection (\#776).
* Handle failures in `submit_workflow` without raising errors (\#787).
* Simplify internal function for execution of a list of task (\#780).
* Exclude `common/tests` and other git-related files from build (\#795).
* Remove development dependencies `Pillow` and `pytest-mock` (\#795).
* Remove obsolete folders from `tests/data` folder (\#795).

# 1.3.3

* Pin Pydantic to v1 (\#779).

# 1.3.2

* Add sqlalchemy naming convention for DB constraints, and add `render_as_batch=True` to `do_run_migrations` (\#757).
* Fix bug in job-stop endpoint, due to missing default for `FractalSlurmExecutor.wait_thread.shutdown_file` (\#768, \#769).
* Fix bug upon inserting a task with `meta=None` into a Workflow (\#772).

# 1.3.1

* Fix return value of stop-job endpoint (\#764).
* Expose new GET `WorkflowTask` endpoint (\#762).
* Clean up API modules (\#762):
    * Split workflow/workflowtask modules;
    * Split tasks/task-collection modules.

# 1.3.0

* Refactor user model:
    * Switch from UUID4 to int for IDs (\#660, \#684).
    * Fix many-to-many relationship between users and project (\#660).
    * Rename `Project.user_member_list` into `Project.user_list` (\#660).
    * Add `username` column (\#704).
* Update endpoints (see also [1.2->1.3 upgrade info](../internals/version_upgrades/upgrade_1_2_5_to_1_3_0/) in the documentation):
    * Review endpoint URLs (\#669).
    * Remove foreign keys from payloads (\#669).
* Update `Task` models, task collection and task-related endpoints:
    * Add `version` and `owner` columns to `Task` model (\#704).
    * Set `Task.version` during task collection (\#719).
    * Set `Task.owner` as part of create-task endpoint (\#704).
    * For custom tasks, prepend `owner` to user-provided `source` (\#725).
    * Remove `default_args` from `Tasks` model and from manifest tasks (\#707).
    * Add `args_schema` and `args_schema_version` to `Task` model (\#707).
    * Expose `args_schema` and `args_schema_version` in task POST/PATCH endpoints (\#749).
    * Make `Task.source` task-specific rather than package-specific (\#719).
    * Make `Task.source` unique (\#725).
    * Update `_TaskCollectPip` methods, attributes and properties (\#719).
    * Remove private/public options for task collection (\#704).
    * Improve error message for missing package manifest (\#704).
    * Improve behavior when task-collection folder already exists (\#704).
    * Expose `pinned_package_version` for tasks collection (\#744).
    * Restrict Task editing to superusers and task owners (\#733).
    * Implement `delete_task` endpoint (\#745).
* Update `Workflow` and `WorkflowTask` endpoints:
    * Always merge new `WorkflowTask.args` with defaults from `Task.args_schema`, in `update_workflowtask` endpoint (\#759).
    * Remove `WorkflowTask.overridden_meta` property and on-the-fly overriding of `meta` (\#752).
    * Add warning when exporting workflows which include custom tasks (\#728).
    * When importing a workflow, only use tasks' `source` values, instead of `(source,name)` pairs (\#719).
* Job execution:
    * Add `FractalSlurmExecutor.shutdown` and corresponding endpoint (\#631, \#691, \#696).
    * In `FractalSlurmExecutor`, make `working_dir*` attributes required (\#679).
    * Remove `ApplyWorkflow.overwrite_input` column (\#684, \#694).
    * Make `output_dataset_id` a required argument of apply-workflow endpoint (\#681).
    * Improve error message related to out-of-space disk (\#699).
    * Include timestamp in job working directory, to avoid name clashes (\#756).
* Other updates to endpoints and database:
    * Add `ApplyWorkflow.end_timestamp` column (\#687, \#684).
    * Prevent deletion of a `Workflow`/`Dataset` in relationship with existing `ApplyWorkflow` (\#703).
    * Add project-name uniqueness constraint in project-edit endpoint (\#689).
* Other updates to internal logic:
    * Drop `WorkflowTask.arguments` property and `WorkflowTask.assemble_args` method (\#742).
    * Add test for collection of tasks packages with tasks in a subpackage (\#743).
    * Expose `FRACTAL_CORS_ALLOW_ORIGIN` environment variable (\#688).
    * Expose `FRACTAL_DEFAULT_ADMIN_USERNAME` environment variable (\#751).
* Package and repository:
    * Remove `fastapi-users-db-sqlmodel` dependency (\#660).
    * Make coverage measure more accurate (\#676) and improve coverage (\#678).
    * Require pydantic version to be `>=1.10.8` (\#711, \#713).
    * Include multiple `fractal-common` updates (\#705, \#719).
    * Add test equivalent to `alembic check` (\#722).
    * Update `poetry.lock` to address security alerts (\#723).
    * Remove `sqlmodel` from `fractal-common`, and declare database models with multiple inheritance (\#710).
    * Make email generation more robust in `MockCurrentUser` (\#730).
    * Update `poetry.lock` to `cryptography=41`, to address security alert (\#739).
    * Add `greenlet` as a direct dependency (\#748).
    * Removed tests for `IntegrityError` (\#754).


# 1.2.5

* Fix bug in task collection when using sqlite (\#664, \#673).
* Fix bug in task collection from local package, where package extras were not considered (\#671).
* Improve error handling in workflow-apply endpoint (\#665).
* Fix a bug upon project removal in the presence of project-related jobs (\#666). Note: this removes the `ApplyWorkflow.Project` attribute.

# 1.2.4

* Review setup for database URLs, especially to allow using UNIX-socket connections for postgresl (\#657).

# 1.2.3

* Fix bug that was keeping multiple database conection open (\#649).

# 1.2.2

* Fix bug related to `user_local_exports` in SLURM-backend configuration (\#642).

# 1.2.1

* Fix bug upon creation of first user when using multiple workers (\#632).
* Allow both ports 5173 and 4173 as CORS origins (\#637).

# 1.2.0

* Drop `project.project_dir` and replace it with `user.cache_dir` (\#601).
* Update SLURM backend (\#582, \#612, \#614); this includes (1) combining several tasks in a single SLURM job, and (2) offering more granular sources for SLURM configuration options.
* Expose local user exports in SLURM configuration file (\#625).
* Make local backend rely on custom `FractalThreadPoolExecutor`, where `parallel_tasks_per_job` can affect parallelism (\#626).
* Review logging configuration (\#619, \#623).
* Update to fastapi `0.95` (\#587).
* Minor improvements in dataset-edit endpoint (\#593) and tests (\#589).
* Include test of non-python task (\#594).
* Move dummy tasks from package to tests (\#601).
* Remove deprecated parsl backend (\#607).
* Improve error handling in workflow-import endpoint (\#595).
* Also show logs for successful workflow execution (\#635).

# 1.1.1

* Include `reordered_workflowtask_ids` in workflow-edit endpoint payload, to reorder the task list of a workflow (\#585).

# 1.1.0

* Align with new tasks interface in `fractal-tasks-core>=0.8.0`, and remove `glob_pattern` column from `resource` database table (\#544).
* Drop python 3.8 support (\#527).
* Improve validation of API request payloads (\#545).
* Improve request validation in project-creation endpoint (\#537).
* Update the endpoint to patch a `Task` (\#526).
* Add new project-update endpoint, and relax constraints on `project_dir` in new-project endpoint (\#563).
* Update `DatasetUpdate` schema (\#558 and \#565).
* Fix redundant task-error logs in slurm backend (\#552).
* Improve handling of task-collection errors (\#559).
* If `FRACTAL_BACKEND_RUNNER=slurm`, include some configuration checks at server startup (\#529).
* Fail if `FRACTAL_SLURM_WORKER_PYTHON` has different versions of `fractal-server` or `cloudpickle` (\#533).

# 1.0.8

* Fix handling of parallel-tasks errors in `FractalSlurmExecutor` (\#497).
* Add test for custom tasks (\#500).
* Improve formatting of job logs (\#503).
* Improve error handling in workflow-execution server endpoint (\#515).
* Update `_TaskBase` schema from fractal-common (\#517).

# 1.0.7

* Update endpoints to import/export a workflow (\#495).

# 1.0.6

* Add new endpoints to import/export a workflow (\#490).

# 1.0.5

* Separate workflow-execution folder into two (server- and user-owned) folders, to avoid permission issues (\#475).
* Explicitly pin sqlalchemy to v1 (\#480).

# 1.0.4

* Add new POST endpoint to create new Task (\#486).

# 1.0.3

Missing due to releasing error.

# 1.0.2

* Add `FRACTAL_RUNNER_MAX_TASKS_PER_WORKFLOW` configuration variable (\#469).

# 1.0.1

* Fix bug with environment variable names (\#468).

# 1.0.0

* First release listed in CHANGELOG.
