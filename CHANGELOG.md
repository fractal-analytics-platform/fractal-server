**Note**: Numbers like (\#1234) point to closed Pull Requests on the fractal-server repository.

# 2.19.4 (unreleased)

* Database:
    * Move some non-duplication constraints on `TaskGroupV2` from aux functions to database level (\#3185).

# 2.19.3

* Task lifecycle:
    * Expose `preamble` for pixi/SLURM task collection (\#3182).

# 2.19.2

* Task lifecycle:
    * Support both `--mem` and `--mem-per-cpu` for pixi/SLURM task collection (\#3180).
* Runner:
    * Also log workflowtask `alias` field, if present (\#3179).
* API:
    * Add regex validator to `Project` and `Dataset` schemas (\#3175).
* Dependencies:
    * Bump `gunicorn` to v25 (\#3174).
    * Bump `packaging` to v26 (\#3178).

# 2.19.1

* Runner:
    * Expand `STDERR_IGNORE_PATTERNS` for SLURM errors with `srun: warning: can't run 1 processes on 8 nodes, setting nnodes to 1` (\#3169).
    * Expose SLURM `nodes` parameter (\#3171).
* Dependencies:
    * Bump documentation dependencies (\#3172).

# 2.19.0

* API:
    * Add `POST /admin/v2/linkuserproject/verify/` (\#3130).
    * Include new `is_guest` field in users CRUD (\#3130).
    * Return empty list of project-sharing invitations for `is_guest=True` users (\#3130).
    * Rename `current_user_act_ver_prof` into `get_api_guest`, and add a more restrictive `get_api_user` dependency for non-read-only endpoints (\#3130).
    * Make `UserManager.validate_password` fail if password is longer than 72 bytes (\#3141).
    * Prevent guest users from self update (\#3142).
    * Deprecate use of `TaskV2.source` in workflow imports and superuser-only task queries(\#3147, \#3148).
    * Improve definition of *latest version* in task import (\#3153).
    * Remove `GET /project/{project_id}/status-legacy/` endpoint (\#3160).
* Database:
    * Drop `TaskV2.source` (\#3147).
    * Add `UserOAuth.is_guest` boolean column and corresponding `CHECK` constraint (\#3130).
    * Add `description` to workflow and `description` and `alias` to workflow task (\#3156, \#3164).
    * Drop `DatasetV2.history` (\#3160).
    * Add `fractal_server_version` column to `JobV2` and `TaskGroupActivityV2` (\#3161).
* Internal:
    * Refactor modules for endpoints in `/api/` but not in `/api/v2/` (\#3132).
* Testing:
    *  Run some GitHub Actions when targeting `ihb-develop` branch (\#3138).
* Documentation:
    * Introduce `CONTRIBUTING.md` (\#3157).
* Dependencies:
    * Bump `gunicorn` to v24 (\#3158).
    * Remove `pre-commit` from project dependencies (\#3157).

# 2.18.6

* API:
    * Drop all redundant `db.close()` statements (\#3118).
    * Expunge items before `setattr` to prevent `StaleDataError` (\#3118).
* Database:
    * Use `autoflush=True` also for sync db sessions (\#3119).
* Dependencies:
    * Drop `mypy` dev dependency (\#3123).
    * Drop support for Python 3.11 (\#3129).
* Testing:
    * Implement some more `zizmor` recommendations (\#3121, \#3122, \#3124, \#3125).
    * Remove `GitHubSecurityLab/actions-permissions/monitor` to avoid TLS issues (\#3124).
    * Bump `uv` version in GitHub Actions (\#3124).

# 2.18.5

* API:
    * Skip email-sending logic when email settings are not present (\#3102).
    * Clarify viewer-path endpoint (\#3096).
* Internals:
    * Fix bug where `merge_type_filters` would mutate one of its arguments (\#3116).
* Documentation:
    * Fix OAuth API documentation (\#3106).
* Testing:
    * Add script to run OAuth tests locally (\#3110).
    * Fix OAuth test by including `fastapi-users` cookies (\#3110).
* Dependencies:
    * Bump `fastapi` to v0.128 (\#3110).
    * Bump `sqlmodel` to v0.0.31 (\#3110).
    * Bump `fastapi-users` to v15.0.3 (\#3110).
    * Bump `uvicorn` to v0.40 (\#3110).
    * Bump `pydantic-settings` to v2.12.0 (\#3110).

# 2.18.4

* Include `LICENSE` file in distribution (\#3099, \#3100).

# 2.18.3

* API:
    * Add `resource_id` query parameter for admin jobs endpoint (\#3082).
    * Support custom email claim in OIDC provider (\#3095).
* Runner:
    * Include `use_mem_per_cpu: bool = False` flag for SLURM configuration (\#3078).
    * Replace large traceback with placeholder (\#3081).
* Settings:
    * Introduce `OAuthSettings.OAUTH_EMAIL_CLAIM` (\#3095).
* Dependencies:
    * Move from `poetry` optional dependency groups to standard dependency groups for `dev` and `docs` (\#3086).
* Internal:
    * Move `fractal_server` to `src/fractal_server` (\#3086). Reverted with \#3092.
    * Move from `poetry` to `uv` (\#3086, \#3088, \#3089).
* Testing:
    * Implement several `zizmor` recommendations (\#3077).
    * Add test for include/exclude options in build backend (\#3089).

# 2.18.2

* API:
    * Review logs in `read_log_file` aux function (\#3073).
    * Review the job-list endpoints, so that their response is sorted and they do not check for project ownership information (\#3076).
* Database:
    * Drop `TaskGroupV2.venv_size_in_kB` and `TaskGroupV2.venv_file_number` (\#3075).
* Task-group lifecycle:
    * Stop measuring venv size and venv file number (\#3075).

# 2.18.1

* Database:
    * Drop `server_default` for `is_owner`, `is_verified` and `permissions` in `LinkUserProjectV2` (\#3058).
    * Drop `server_default` for `UserOAuth.project_dirs` and drop column `UserOAuth.project_dir` (\#3058).

# 2.18.0

> NOTE: This version requires running a data-migration script (`fractalctl update-db-data`).

> WARNING: Before upgrading to this version, make sure that no jobs are marked as submitted in the current database tables.

The main contents of this release are the introduction of the project sharing and a review of the authorization scheme for [`fractal-data`](https://github.com/fractal-analytics-platform/fractal-data).

* API:
    * Add project-owner endpoints `/api/v2/project/{project_id}/guest/` (\#2999).
    * Add project-guest endpoints `/api/v2/project/invitation/`, `/api/v2/project/{project_id}/access/` and `/api/v2/project/{project_id}/access/accept/` (\#2999).
    * Add project-sharing admin endpoint (\#2999).
    * Add granular access-control rules to `/api/v2` endpoints, valid for both project owners and guests (\#2999, \#3029).
    * Add pagination to `GET /admin/v2/task-group/activity/` and `GET /admin/v2/task-group/` (\#3023).
    * Do not cast endpoint return values to `PaginationResponse[X]` (\#3023).
    * Reduce API logging level for some endpoints (\#3010).
    * Modify `GET /auth/current-user/allowed-viewer-paths/` logic, with `include_shared_projects` query parameter (\#3031, \#3069).
    * Add validator for paths to forbid parent-directory references (\#3031).
    * Add check to `PATCH /auth/users/{user_id}/` when patching `project_dirs` (\#3043, \#3069).
    * Review job-submission endpoint (\#3041).
    * Prevent submissions if `Resource.prevent_new_submissions` is set (\#3042).
    * Add `normpath` to `AbsolutePathStr` type (\#3050).
    * Split `zarr_dir` request body argument of `POST /project/{project_id}/dataset/` into `project_dir` and `zarr_subfolder` (\#3051).
    * Add `zarr_dir` validity check in `POST /project/{project_id}/dataset/import/` (\#3051).
    * Remove `zarr_dir` request body argument from `PATCH /project/{project_id}/dataset/{dataset_id}/` (\#3051).
    * Validate `images` in `DatasetImport` (\#3057).
    * Sort response of `GET /admin/v2/task-group/activity/` starting with the most recent activities (\#3066).
* App:
    * Add `SlowResponseMiddleware` middleware (\#3035, \#3038, \#3060).
* Settings:
    * Add `Settings.FRACTAL_LONG_REQUEST_TIME` configuration variable (\#3035).
* Runner:
    * Improve logging for when dataset or workflow is not found in the database (\#3046).
    * Prevent job-execution from continuing if `Resource.prevent_new_submissions` is set (\#3042).
    * Espose `shebang_line` in resource runner configuration (\#3064).
* Database:
    * Add `Resource.prevent_new_submissions` boolean flag (\#3042).
    * Add project-sharing-related `LinkUserProjectV2` columns (\#2999).
    * Move `UserOAuth.project_dir` to `.project_dirs` and drop `UserGrop.viewer_paths` (\#3031).
    * Enforce max one submitted `JobV2` per `DatasetV2` (\#3044).
    * Create 2.18.0 data-migration script (\#3031, \#3050).
* Settings:
    * Drop `DataSettings` (\#3031).
    * Reduce API logging level for some endpoints (\#3010).
* Internal:
    * Remove the "V2" label from names of internal schemas and API route tags (\#3037).
* Testing:
    * Expand SLURM-batching-heuristics test (\#3011).
    * Also validate resources and profiles in `migrations.yml` action (\#3046).
    * Replace `MockCurrentUser.user_kwargs` with explicit keyword arguments (\#3061).
* Dependencies:
    * Support Python 3.14 (\#3015).

# 2.17.2

> NOTE: Starting from this version, the `unzip` command-line tool must be available (see \#2978).

* API:
    * Allow reading logs from zipped job folders (\#2978).
    * Do not raise 422 from check-owner functions (\#2988).
    * Remove `GET /admin/v2/project/`, `GET /api/v2/dataset/` and `GET /api/v2/workflow/` (\#2989).
* Database:
    * Add indexes to `HistoryImageCache` and `HistoryUnit` foreign keys to improve `DatasetV2` deletion time (\#2987).
    * Drop `ProjectV2.user_list` relationship (\#2991).
    * Adopt `onclause` on `JOIN` statements (\#2993).
* Runner:
    * Handle default `batch_size` value for local runner (\#2949).
    * Rename `SudoSlurmRunner` into `SlurmSudoRunner` (\#2980).
    * Ignore some SLURM/stderr patterns when populating `executor_error_log` (\#2984).
* App settings:
    * Transform `DB_ECHO` configuration variable from boolean to `Literal['true', 'false']` (\#2985).
* Documentation:
    * Major review of documentation, including making it up-to-date with v2.17.0 and relying more on autogenerated contents (\#2949, \#2983).
* Development:
    * Add `shellcheck` to precommit, for `fractal-server/` files (\#2986).
    * Replace `reorder_python_imports`, `flake8` and `black` with `ruff` in pre-commit (\#2994).
* Task-group lifecycle:
    * Support extras in pinned packages names (\#3000).
* Testing:
    * Update benchmarks (\#2990).
    * Drop `benchmarks_runner` (\#2990).

# 2.17.1

* Runner:
    * Raise an error for a non-converter task running on an empty image list (\#2971).
* Database:
    * Apply all database-schema changes made possible by 2.17.0 data migration (\#2972), namely:
        * Drop `user_settings` table and corresponding `UserOAuth.user_settings_id` foreign key.
        * Make `resource_id` foreign key non nullable in `ProjectV2` and `TaskGroupV2` tables.
        * Drop `server_default="/PLACEHOLDER` for `UserOAuth.project_dir`.
* App:
    * Streamline graceful-shutdown logic in lifespan (\#2972).
* Settings:
    * Accept float values for `FRACTAL_GRACEFUL_SHUTDOWN_TIME` (\#2972).
* Testing:
    * Update testing database with 2.17.0 data migration (\#2974).
    * Introduce `generic_task_converter` in `fractal-tasks-mock` (\#2976).

# 2.17.0

> This version requires running a data-migration script (`fractalctl update-db-data`),
> see [detailed
> instructions](https://fractal-analytics-platform.github.io/fractal-server/internals/version_upgrades/upgrade_2_16_6_to_2_17_0/).

The main content of this release is the introduction of the computational
resource&profile concepts, and a review of the application settings.

* API (main PRs: \#2809, \#2870, \#2877, \#2884, \#2911, \#2915, \#2925, \#2940, \#2941, \#2943, \#2956):
    * Introduce API for `Resource` and `Profile` models.
    * Drop API for user settings.
    * Drop handling of deprecated `DatasetV2.filters` attribute when creating dataset dumps (\#2917).
    * Enable querying users by `resource_id` (\#2877).
    * Check matching-`resource_id` upon job submission (\#2896).
    * Treat `TaskGroupV2.resource_id` as not nullable (\#2896).
    * Split `/api/settings/` into smaller-scope endpoints.
    * Update rules for read access to task groups with resource information (\#2941).
    * Only show tasks and task groups associated to current user's `Resource` (\#2906, \#2943).
    * Add pagination to admin job and task endpoints (\#2958).
    * Add `task_type` query parameter to admin task endpoint (\#2958).
* Task-group lifecycle:
    * Rely on resource and profile rather than user settings (\#2809).
    * Fix postponed commit after task-group deletion (\#2964).
    * Add Python3.14 option for task collection (\#2965).
* Runner
    * Rely on resource and profile rather than user settings (\#2809).
    * Make `extra_lines` a non-optional list in SLURM configuration (\#2893).
    * Enable `user_local_exports` on SLURM-SSH runner.
* Database and models (also \#2931):
    * Introduce `Resource` and `Profile` models (\#2809).
    * Introduce `resource_id` foreign key for task-group and project models (\#).
    * Move `project_dir` and `slurm_accounts` from `UserSettings` to `UserOAuth`.
    * Make `project_dir` required.
    * Discontinue usage of `UserSettings` table.
    * Add data-migration script for version 2.17.0 (\#2933).
* Authentication API:
    * Drop OAuth-based self registration (\#2890).
* App settings (\#2874, \#2882, \#2895, \#2898, \#2916, \#2922, \#2968):
    * Remove all configuration variables that are now part of `Resource`s.
    * Split main `Settings` model into smaller-scope models.
    * Remove email-password encryption.
    * Introduce `init-db-data` command.
    * Set default `FRACTAL_API_MAX_JOB_LIST_LENGTH` to 25 (\#2928).
    * Introduce `FRACTAL_DEFAULT_GROUP_NAME`, set to either `"All"` or `None` (\#2939).
* Dependencies:
    * Bump `fastapi` to v0.120 (\#2921).
    * Bump `uvicorn` to v0.38 (\#2921).
    * Bump `fastapi-users` to v15 (\#2907).
* Testing and GitHub actions:
    * Simplify Python environment in documentation GitHub action (\#2919).
    * Simplify PyPI-publish GitHub action (\#2927).
    * Drop explicit dependency on `python-dotenv (\#2921).
* Testing:
    * Introduce `pytest-env` dependency.
    * Update testing database to version 2.16.6 (\#2909).
    * Test OAuth flow with `pytest` and remove OAuth GHA (\#2929).



# 2.16.6

* API:
    * Fix bug in import-workflow endpoint, leading to the wrong task-group being selected (\#2863).
* Models:
    * Fix use of custom `AttributeFilters` type in SQLModel model (\#2830).
* Dependencies:
    * Bump `pydantic` to 2.12.0 (\#2830).
    * Bump `poetry` to 2.2.1 in GitHub actions (\#2830).
* Testing:
    * Add pre-commit rule to prevent custom types is models (\#2830).

# 2.16.5

* Dependencies:
    * Bump `fastapi`, `sqlmodel`, `uvicorn`, `pydantic-settings` versions (\#2827).

# 2.16.4

* Task life cycle:
    * Switch to PyPI Index API for finding latest package versions (\#2790).
* SSH:
    * Bump default lock-acquisition timeout from 250 to 500 seconds (\#2826).
    * Introduce structured logs for SSH-lock dynamics (\#2826).
* API:
    * Replace `HTTP_422_UNPROCESSABLE_CONTENT` with `HTTP_422_UNPROCESSABLE_CONTENT` (\#2790).
* Internal:
    * Move `app.runner` into `runner` (\#2814).
* Testing:
    * Use function-scoped base folder for backend (\#2793).
* Dependencies:
    * Bump `fastapi` version (\#2790).
    * Bump `uvicorn-worker` and `sqlmodel` versions (\#2792).

# 2.16.3

* Task life cycle:
    * Move post-pixi-installation logic into the same SLURM job as `pixi install`, for SSH/SLURM deployment (\#2787).
* Dependencies:
    * Bump `cryptography` (\#2788).

# 2.16.2

* Task life cycle:
    * Move `pixi install` execution to SLURM jobs, for SSH/SLURM deployment (\#2784).
    * Add `SLURM_CONFIG` to `PixiSettings` (\#2784).

# 2.16.1

* Runner:
    * Drop top-level executor-error-related logs (\#2783).
    * Drop obsolete `FRACTAL_SLURM_SBATCH_SLEEP` configuration variable (\#2785).

# 2.16.0

* Runner:
    * Record SLURM-job stderr in `JobV2.executor_error_log` (\#2750, \#2771, \#2773).
* Task life cycle:
    * Support both pre-pinning and post-pinning of dependencies (\#2761).
    * Drop `FRACTAL_MAX_PIP_VERSION` configuration variable (\#2766).
    * Make TaskGroup deletion a lifecycle operation (\#2759).
* Testing:
    * Add `out_of_memory` mock task (\#2770).
    * Make fixtures sync when possible (\#2776).
    * Bump pytest-related dependencies (\#2776).

# 2.15.9

* API:
    * In `POST /api/v2/project/{project_id}/status/images/`, include _all_ available types&attributes (\#2762).
* Internal:
    * Optimize `fractal_server.images.tools.aggregate_attributes` (\#2762).

# 2.15.8

* Runner:
    * Split `SlurmJob` submission into three steps, reducing SSH connections (\#2749).
    * Deprecate experimental `pre_submission_commands` feature (\#2749).
* Documentation:
    * Fix `Fractal Users` documentation page (\#2738).
    * Improve documentation for Pixi task collection (\#2742).
* Task life cycle:
    * Add test to lock `FRACTAL_MAX_PIP_VERSION` with latest version on PyPI (\#2752).
    * Use pixi v0.54.1 in tests (\#2758).
* Internal
    * Improve type hints (\#2739).
* SSH:
    * Add a decorator to open a new connection (socket) if the decorated function hits a `NoValidConnectionError` or a `OSError` (\#2747).
    * Remove multiple-attempts logic (\#2747).

# 2.15.7

* API:
    * Capture SSH related failure in submit-job endpoint (\#2734).
* Task life cycle:
    * Also edit `tool.pixi.project.platforms` section of `pyproject.toml`, for `pixi`-based task collection (\#2711).
* Internal:
    * Extend use of `UnreachableBranchError` in API and runner (\#2726).
* Dependencies:
    * Bump `fastapi` to version `0.116.*` (\#2718).
    * Bump all `docs` optional dependencies (\#2722).
    * Add support for Python 3.13 (\#2716).
* Documentation:
    * Remove/fix several obsolete docs pages (\#2722).
* Testing:
    * Improve/optimize several tests (\#2727).
    * Use `poetry` 2.1.3 in all GitHub Actions and always install it using `pipx` (\#2731).
    * Move all GitHub Action runners to `ubuntu-24.04` (\#2732).
    * Avoid using `threading` private method (\#2733).

# 2.15.6

* Runner:
    * Remove obsolete `JobExecutionError` attributes and `TaskExecutionError` handling (\#2708).
    * Always interpret `needs_gpu` string as a boolean (\#2706).
* Task life cycle:
    * Use `bash --login` for `pixi install` execution over SSH (\#2709).
* Development:
    * Move `example` folder to root directory (\#2720).

# 2.15.5

* API:
    * Update `HistoryRun` and `HistoryUnit`s statuses when `Workflow`s are manually labeled as failed (\#2705).

# 2.15.4

* Task lifecycle:
    * Edit `pyproject.toml` in-place before running `pixi install` (\#2696).

# 2.15.3

* API:
    * Ongoing `WorkflowTask`s are marked as submitted during `Job` execution (\#2692).

# 2.15.2

* API:
    * Improve logging for `PATCH /admin/v2/job/{job_id}/` (\#2686).
    * Prevent deletion and reordering of `WorkflowTask`s in `Workflow`s associated to submitted `Job`s (\#2689).
* Database:
    * Set `pool_pre_ping=True` for sync db engine (\#2676).
* Runner:
    * Update `chmod ... -R` to `chmod -R ...` (\#2681).
    * Add custom handling of some `slurm_load_jobs` socket-timeout error (\#2683).
    * Remove redundant `mkdir` in SLURM SSH runner (\#2671).
    * Do not write to SLURM stderr from remote worker (\#2691).
    * Fix spurious version-mismatch warning in remote worker (\#2691).
* SSH:
    * Always set `in_stream=False` for `fabric.Connection.run` (\#2694).
* Testing:
    * Fix `test_FractalSSH.py::test_folder_utils` for MacOS (\#2678).
    * Add pytest marker `fails_on_macos` (\#2681).
    * Remove patching of `sys.stdin`, thanks to updates to `fabric.Connection.run` arguments (\#2694).

# 2.15.1

This release fixes the reason for yanking 2.15.0.

* Database:
    * Modify `JSON->JSONB` database schema migration in-place, so that columns which represent JSON Schemas or `meta_*` fields remain in JSON form rather than JSONB (\#2664, \#2666).

# 2.15.0 [yanked]

> This release was yanked on PyPI, because its conversion of all JSON columns
> into JSONB changes the key order, see
> https://github.com/fractal-analytics-platform/fractal-server/issues/2663.


* Database:
    * Rename `TaskGroupV2.wheel_path` into `TaskGroupV2.archive_path` (\#2627).
    * Rename `TaskGroupV2.pip_freeze` into `TaskGroupV2.env_info` (\#2627).
    * Add `TaskGroupV2.pixi_version` (\#2627).
    * Transform every JSON column to JSONB (\#2662).
* API:
    * Introduce new value `TaskGroupV2OriginEnum.PIXI` (\#2627).
    * Exclude `TaskGroupV2.env_info` from API responses (\#2627).
    * Introduce `POST /api/v2/task/collect/pixi/` (\#2627).
    * Extend deactivation/reactivation endpoints to pixi task groups (\#2627).
    * Block `DELETE /api/v2/task-group/{task_group_id}/` if a task-group activity is ongoing (\#2642).
    * Introduce `_verify_non_duplication_group_path` auxiliary function (\#2643).
* Task lifecycle:
    * Introduce full support for pixi task-group lifecycle (\#2627, \#2651, \#2652, \#2654).
* SSH:
    * Introduce `FractalSSH.read_remote_text_file` (\#2627).
* Runner:
    * Fix use of `worker_init/extra_lines` for multi-image job execution (\#2660).
    * Support SLURM configuration options `nodelist` and `exclude` (\#2660).
* App configuration:
    * Introduce new configuration variable `FRACTAL_PIXI_CONFIG_FILE` and new attribute `Settings.pixi` (\#2627, \#2650).


# 2.14.16

* Internal:
    * Refactor and optimize enrich-image functions (\#2620).
* Database:
    * Add indices to `HistoryImageCache` table (\#2620).
* SSH:
    * Increase Paramiko `banner_timeout` from the default 15 seconds to 30 seconds (\#2632).
    * Re-include `check_connection` upon `SlurmSSHRunner` startup (\#2636).
* Testing:
    * Introduce benchmarks for database operations (\#2620).
* Dependencies:
    * Update `cryptography`, `packaging` and `python-dotenv` dependencies (\#2630).

# 2.14.15

* API:
    * Add required `workflowtask_id` query parameter to `verify-unique-types` endpoint (\#2619).
    * Enrich images with status within `verify-unique-types` endpoint, when necessary (\#2619).

# 2.14.14

* API:
    * Fix `GET /api/v2/task-group/` by adding missing sorting before `itertools.groupby` (\#2614).
* Internal:
    * Drop `execute_command_async` function (\#2611).
    * Introduce `TaskType` enum (\#2612).

# 2.14.13

* API:
    * Group response items of `GET /api/v2/task-group/` by `pkg_name` (\#2596).
    * Disambiguate response items of `GET /api/v2/task-group/` (\#2596).
* Internal:
    * Introduce `UnreachableBranchError` (\#2596).
* Testing:
    * Enforce task-group non-duplication constraints in `task_factory_v2` fixture (\#2596).

# 2.14.12

* Runner:
    * Enable status-based selection of images to process (\#2588).
* API:
    * Remove `unit_status` query parameter from `/project/{project_id}/status/images/` (\#2588).
    * Remove default type filters from `/project/{project_id}/status/images/` (\#2588).
    * Sort lists of existing attribute values in `aggregate_attributes` (\#2588).
* Task-group lifecycle:
    * Split `pip install` command into two steps (\#2600).

# 2.14.11

* Task-group lifecycle:
    * Support version-pinning for two dependencies in task collection (\#2590, \#2599).
    * Support version-pinning for previously-missing dependencies in task collection (\#2590, \#2599).
* Development:
    * Improve `mypy` configuration in `pyproject.toml` (\#2595).

# 2.14.10

> This version requires a data-migration script (`fractalctl update-db-data`).

* Database:
    * Improve data-migration script that is necessary for 2.14.8 (\#2594).

# 2.14.9

> WARNING: Do not release this version, but go directly to 2.14.10.

* Task-group lifecycle:
    * Improve handling of SSH-related errors (\#2589).
* Database:
    * Rename data-migration script that is necessary for 2.14.8 (\#2592).

# 2.14.8

> WARNING: Do not release this version, but go directly to 2.14.10.

* API:
    * Update `POST /project/{project_id}/workflow/{workflow_id}/wftask/replace-task/` so that it re-uses existing workflow task (\#2565).
* Database:
    * Add `HistoryRun.task_id` column (\#2565).
* Internal:
    * Refactor: extract `enrich_image_list` function from `/project/{project_id}/status/images/` endpoint (\#2585).

# 2.14.7


* Runner:
    * Re-include SLURM accounts for both sudo-slurm and ssh-slurm runners (\#2580)
    * Re-include use of `worker_init` for both sudo-slurm and ssh-slurm runners (\#2580)
* Testing:
    * Use `Optional` for argument type hints in mock tasks (\#2575).

# 2.14.6

* API:
    * Introduce `api/v2/project/{project.id}/workflow/{workflow.id}/version-update-candidates/` endpoint (\#2556).
* Task lifecycle:
    * Use dedicated SSH connections for lifecycle background tasks (\#2569).
    * Also set `TaskGroup.version` for custom task collections (\#2573).
* Internal:
    * Inherit from `StrEnum` rather than `str, Enum` (\#2561).
    * Run `pyupgrade` on codebase (\#2563).
    * Remove init file of obsolete folder (\#2571).
* Dependencies:
    * Deprecate Python 3.10 (\#2561).

# 2.14.5

This version introduces an important _internal_ refactor of the runner
component, with the goal of simplifying the SLURM version.

* Runner:
    * Make `submit/multisubmit` method take static arguments, rather than a callable (\#2549).
    * Replace input/output pickle files with JSON files (\#2549).
    * Drop possibility of non-`utf-8` encoding for `_run_command_as_user` function (\#2549).
    * Avoid local-remote-local round trip for task parameters, by writing the local file first (\#2549).
    * Stop relying on positive/negative return codes to produce either `TaskExecutionError`/`JobExecutionError`, in favor of only producing `TaskExecutionError` at the lowest task-execution level (\#2549).
    * Re-implement `run_single_task` within SLURM remote `worker` function (\#2549).
    * Drop `TaskFiles.remote_files_dict` (\#2549).
    * Drop obsolete `FRACTAL_SLURM_ERROR_HANDLING_INTERVAL` config variable (\#2549).
    * Drop obsolete `utils_executors.py` module (\#2549).
* Dependencies:
    * Drop `cloudpickle` dependency (\#2549).
    * Remove copyright references to `clusterfutures` (\#2549).
* Testing:
    * Introduce `slurm_alive` fixture that checks `scontrol ping` results (\#2549).

# 2.14.4

* API:
    * Replace most `field_validator`s with `Annotated` types, and review `model_validator`s (\#2504).
* SSH:
    * Remove Python-wrapper layer for `tar` commands (\#2554).
    * Add `elapsed` information to SSH-lock-was-acquired log (\#2558).

# 2.14.3

* Runner:
    * Skip creation/removal of folder copy in compress-folder module (\#2553).
    * Drop obsolete `--extra-import-paths` option from SLURM remote worker (\#2550).

# 2.14.2

* API:
    * Handle inaccessible `python_interpreter` or `package_root` in custom task collection  (\#2536).
    * Do not raise non-processed-images warning if the previous task is a converter (\#2546).
* App:
    * Use `Enum` values in f-strings, for filenames and error messages (\#2540).
* Runner:
    * Handle exceptions in post-task-execution runner code (\#2543).

# 2.14.1

* API:
    * Add `POST /project/{project_id}/dataset/{dataset_id}/images/non-processed/` endpoint (\#2524, \#2533).
* Runner:
    * Do not create temporary output-pickle files (\#2539).
    * Set logging level to `DEBUG` within `compress_folder` and `extract_archive` modules (\#2539).
    * Transform job-error log into warning (\#2539).
    * Drop `FRACTAL_SLURM_INTERVAL_BEFORE_RETRIEVAL` (\#2525, \#2531).
    * Increase `MAX_NUM_THREADS` from 4 to 12 (\#2520).
    * Support re-deriving an existing image with a non-trivial `origin` (\#2527).
* Testing:
    * Adopt ubuntu24 containers for CI (\#2530).
    * Do not run Python3.11 container CI for PRs, but only for merges (\#2519).
    * Add mock wheel file and update assertion for pip 25.1 (\#2523).
    * Optimize `test_reactivate_local_fail` (\#2511).
    * Replace `fractal-tasks-core` with `testing-tasks-mock` in tests (\#2511).
    * Improve flaky test (\#2513).

# 2.14.0

This release mostly concerns the new database/runner integration in view of
providing more granular history/status information. This includes a full
overhaul of the runner.

* API:
    * Add all new status endpoints.
    * Add `GET /job/latest/` endpoint (\#2389).
    * Make request body required for `replace-task` endpoint (\#2355).
    * Introduce shared tools for pagination.
    * Remove `valstr` validator and introduce `NonEmptyString` in schemas (\#2352).
* Database
    * New tables `HistoryRun`, `HistoryUnit` and `HistoryImageCache` tables.
    * Drop attribute/type filters from dataset table.
    * Add `type_filters` column to job table.
    * Use `ondelete` flag in place of custom DELETE-endpoint logics.
* Runner
    * Full overhaul of runners. Among the large number of changes, this includes:
        * Fully drop the `concurrent.futures` interface.
        * Fully drop the multithreaded nature of SLURM runners, in favor of a more linear submission/retrieval flow.
        * New `BaseRunner`, `LocalRunner`, `BaseSlurmRunner`, `SlurmSSHRunner` and `SlurmSudoRunner` objects.
        * The two SLURM runners now share a large part of base logic.
        * Database updates to `HistoryRun`, `HistoryUnit` and `HistoryImageCache` tables.
        * We do not fill `Dataset.history` any more.
* Task lifecycle:
    * Drop hard-coded use of `--no-cache-dir` for `pip install` command (\#2357).
* App:
    * Obfuscate sensitive information from settings using `SecretStr` (\#2333).
    * Drop `FRACTAL_RUNNER_TASKS_INCLUDE_IMAGE` obsolete configuration variable (\#2359).
* Testing:
    * Use `fractal-task-tools` to build `fractal-tasks-mock` manifest (\#2374).
* Development:
    * Add `codespell` to precommit (\#2358).
    * Drop obsolete `examples` folder (\#2405).


# 2.13.1

* API:
    * Add `AccountingRecord` and `AccountingRecordSlurm` tables (\#2267).
    * Add `/admin/v2/impersonate` endpoint (\#2280).
    * Replace `_raise_if_naive_datetime` with `AwareDatetime` (\#2283).
* Database:
    * Add `/admin/v2/accounting/` and `/admin/v2/accounting/slurm/` endpoints (\#2267).
* Runner:
    * Populate `AccountingRecord` from runner (\#2267).
* App:
    * Review configuration variables for email-sending (\#2269).
    * Reduce error-level log records(\#2282).
* Testing:
    * Drop obsolete files/folders from `tests/data` (\#2281).
* Dependencies:
    * Bump `httpx` to version `0.28.*` (\#2284).

# 2.13.0

With this release we switch to Pydantic v2.

* Runner:
    * Deprecate `FRACTAL_BACKEND_RUNNER="local_experimental"` (\#2273).
    * Fully replace `clusterfutures` classes with custom ones (\#2272).
* Dependencies:
    * Bump `pydantic` to v2 (\#2270).
    * Drop `clusterfutures` dependency (\#2272).
    * Drop `psutil` dependency (\#2273).
    * Bump `cryptography` to version `44.0.*` (\#2274).
    * Bump `sqlmodel` to version `0.0.22` (\#2275).
    * Bump `packaging` to version `24.*.*` (\#2275).
    * Bump `cloudpickle` to version `3.1.*` (\#2275).
    * Bump `uvicorn-workers` to version `0.3.0` (\#2275).
    * Bump `gunicorn` to version `23.*.*` (\#2275).
    * Bump `httpx` to version `0.27.*` (\#2275).

# 2.12.1

> Note: this version requires a manual update of email-related configuration variables.

* API:
    * Deprecate `use_dataset_filters` query parameter for `/project/{project_id}/dataset/{dataset_id}/images/query/` (\#2231).
* App:
    * Add fractal-server version to logs (\#2228).
    * Review configuration variables for email-sending (\#2241).
* Database:
    * Remove `run_migrations_offline` from `env.py` and make `run_migrations_online` sync (\#2239).
* Task lifecycle:
    * Reset logger handlers upon success of a background lifecycle operation, to avoid open file descriptors (\#2256).
* Runner
    * Sudo/SLURM executor checks the fractal-server version using `FRACTAL_SLURM_WORKER_PYTHON` config variable, if set (\#2240).
    * Add `uname -n` to SLURM submission scripts (\#2247).
    * Handle `_COMPONENT_KEY_`-related errors in sudo/SLURM executor, to simplify testing (\#2245).
    * Drop obsolete `SlurmJob.workflow_task_file_prefix` for both SSH/sudo executors (\#2245).
    * Drop obsolete `keep_pickle_files` attribute from slurm executors (\#2246).
* Dependencies:
    * Bump `uvicorn` version (\#2242).
* Testing:
    * Improve testing of sudo-Slurm executor (\#2245, \#2246).
    * Introduce `container` pytest marker (\#2249).
    * Split CI GitHub Actions in three jobs: API, not API and Containers (\#2249).

# 2.12.0

> WARNING: The database schema update introduced via this version is non-reversible.

* API:
    * Drop V1 endpoints (\#2230).
* Database:
    * Drop V1 tables (\#2230).
* Runner:
    * Drop V1 runners (\#2230).
* Testing:
    * Drop V1 tests (\#2230).
    *  Update V2 tests to keep coverage stable (\#2230).


# 2.11.1

* Database
    * Drop columns `DatasetV2.filters` and `WorkflowTaskV2.input_filters` (\#2232).

# 2.11.0

This version revamps the filters data structure, and it introduces complex attribute filters.

> Note: This release requires running `fractalctl update-db-data`.
> Some legacy columns will be removed from the database, either as part of
> the `2.11.0` data-migration or as part of the `2.11.1` schema migration.
> Please make sure you have a database dump.

* API:
    * Align API with new database schemas for filters-related columns (\#2168, \#2196, \#2202).
    * Support importing workflows or datasets with legacy (pre-`2.11.0`) filters-related fields (\#2185, \#2227).
    * Avoid blocking operations from the download-job-logs endpoint, when the zip archive of a running job is requested (\#2225).
    * Update and simplify `/api/v2/project/{project_id}/status/`, dropping use of temporary job files (\#2169).
    * Add new (experimental) `/project/{project_id}/workflow/{workflow_id}/type-filters-flow/` endpoint (\#2208).
* Database:
    * Update table schemas for all filters-related columns:
        * Always handle attribute- and type-filters in different columns (\#2168).
        * Update attribute-filter-values type from scalar to list (\#2168, \#2196).
        * Deprecate attribute filters for `WorkflowTaskV2` (\#2168).
        * Add attribute filters to `JobV2` (\#2168).
    * `2.11.0` data-migration script (\#2168, \#2202, \#2208, \#2209).
* Runner:
    * Introduce database writes in runner component, to replace the use of temporary files (\#2169).
    * Use `TaskV2.input_types` for filtering, rather than validation (\#2191, \#2196).
    * Make job-execution background-task function sync, to make it transparent that it runs on a thread (\#2220).
    * Remove all filters from `TaskOutput` (\#2190).
* Task Collection:
    * Improve logs handling for failed task collections (\#2192)
* Testing:
    * Speed up CI by splitting it into more jobs (\#2210).

# 2.10.6

* Task lifecycle:
    * Use unique logger names for task-lifecycle operations (\#2204).

# 2.10.5

* App:
    * Add missing space in "To" field for email settings (\#2173).
* Testing:
    * Improve configuration for coverage GitHub Action step (\#2175).
    * Add `persist-credentials: false` to `actions/checkout@v4` GitHub Action steps (\#2176).
* Dependencies:
    * Require `bumpver>2024.0` (\#2179).


# 2.10.4

* Switch to poetry v2 (\#2165).
* Require Python <3.13 (\#2165).

# 2.10.3

Note: this version fixes a bug introduced in version 2.10.1.

* API:
    * Fix bug in `POST /api/v2/project/{p_id}/workflow/{w_id}/wftask/replace-task/` endpoint (\#2163).
    * Add validation for `.whl` filename (\#2147).
    * Trim whitespaces in `DatasetCreateV2.zarr_dir` (\#2138).
    * Support sending emails upon new OAuth signup (\#2150).
* App:
    * Introduce configuration for email settings (\#2150).
* Command-line interface:
    * Add `fractalctl email-settings` (\#2150).
* Dependencies:
    * Add direct dependency on `cryptography` (\#2150).
* Testing:
    * Introduce `mailpit`-based end-to-end test of email sending (\#2150).

# 2.10.2

* App:
    * Add `FRACTAL_PIP_CACHE_DIR` configuration variable (\#2141).
* Tasks life cycle:
    * Prevent deactivation of task groups with `"github.com"` in pip-freeze information (\#2144).
* Runner:
    * Handle early shutdown for sudo SLURM executor (\#2132).
    * Fix repeated setting of `timestamp_ended` in task-group reactivation (\#2140).

# 2.10.1

* API:
    * Add `POST /api/v2/project/{p_id}/workflow/{w_id}/wftask/replace-task/` endpoint (\#2129).
* Testing:
    * Use system postgresql in GitHub actions, rather than independent container (\#2199).

# 2.10.0

* API:
    * Major update of `POST /api/v2/task/collect/pip/`, to support wheel-file upload (\#2113).
* Testing:
    * Add test of private task collection (\#2126).

# 2.9.2

* API
    * Remove `cache_dir` and use `project_dir/.fractal_cache` (\#2121).
* Docs
    * Improve docstrings and reduce mkdocs warnings (\#2122).

# 2.9.1

* Task collection:
    * Fix bug in wheel-based SSH task-collection (\#2119).
* Testing:
    * Re-include a specific test previously skipped for Python 3.12 (\#2114).
    * Add metadata to `fractal-tasks-mock` package (\#2117).
* Docs:
    * Add info about working versions.

# 2.9.0

> WARNING 1: This version drops support for sqlite, and removes the
> configuration variables `DB_ENGINE` and `SQLITE_PATH`.

> WARNING 2: This version removes the `CollectionStateV2` database table.
> Make sure you have a database dump before running `fractalctl set-db`, since this operation cannot be undone.

* API
    * Remove `GET /api/v2/task/collect/{state_id}/` endpoint (\#2010).
    * Remove `active` property from `PATCH /api/v2/task-group/{task_group_id}/` (\#2033).
    * Add `GET /api/v2/task-group/activity/` endpoint (\#2005, \#2027).
    * Add `GET /api/v2/task-group/activity/{task_group_activity_id}/` endpoint (\#2005).
    * Add `GET /admin/v2/task-group/activity/` endpoint (\#2005, \#2027).
    * Add `POST /api/v2/task-group/{task_group_id}/{deactivate|reactivate}` endpoints (\#2033, \#2066, \#2078).
    * Add `POST /admin/v2/task-group/{task_group_id}/{deactivate|reactivate}` endpoints (\#2062, \#2078).
    * Remove `GET /auth/current-user/viewer-paths/` (\#2096).
    * Add `GET /auth/current-user/allowed-viewer-paths/`, with logic for `fractal-vizarr-viewer` authorization (\#2096).
    * Add `category`, `modality` and `author` query parameters to `GET /admin/v2/task/` (\#2102).
    * Add `POST /auth/group/{group_id}/add-user/{user_id}/` (\#2101).
    * Add `POST /auth/group/{group_id}/remove-user/{user_id}/` (\#2101, \#2111).
    * Add `POST /auth/users/{user_id}/set-groups/` (\#2106).
    * Remove `new_user_ids` property from `PATCH /auth/group/{group_id}/` (\#2101).
    * Remove `new_group_ids` property from `PATCH /auth/users/{user_id}/` (\#2106).
    * Internals:
      * Fix bug in `_get_collection_task_group_activity_status_message` (\#2047).
      * Remove `valutc` validator for timestamps from API schemas, since it does not match with `psycopg3` behavior (\#2064).
      * Add query parameters `timestamp_last_used_{min|max}` to `GET /admin/v2/task-group/` (\#2061).
      * Remove `_convert_to_db_timestamp` and add `_raise_if_naive_datetime`: now API only accepts timezone-aware datetimes as query parameters (\#2068).
      * Remove `_encode_as_utc`: now timestamps are serialized in JSONs with their own timezone (\#2081).
* Database
    * Drop support for sqlite, and remove the `DB_ENGINE` and `SQLITE_PATH` configuration variables (\#2052).
    * Add `TaskGroupActivityV2` table (\#2005).
    * Drop `CollectionStateV2` table (\#2010).
    * Add `TaskGroupV2.pip_freeze` nullable column (\#2017).
    * Add  `venv_size_in_kB` and `venv_file_number` to `TaskGroupV2` (\#2034).
    * Add `TaskGroupV2.timestamp_last_used` column, updated on job submission (\#2049, \#2061, \#2086).
* Task-lifecycle internals:
    * Refactor task collection and database-session management in background tasks (\#2030).
    * Update `TaskGroupActivityV2` objects (\#2005).
    * Update filename and path for task-collection scripts (\#2008).
    * Copy wheel file into `task_group.path` and update `task_group.wheel_path`, for local task collection (\#2020).
    * Set `TaskGroupActivityV2.timestamp_ended` when collections terminate (\#2026).
    * Refactor bash templates and add `install_from_freeze.sh` (\#2029).
    * Introduce background operations for _local_ reactivate/deactivate (\#2033).
    * Introduce background operations for _SSH_ reactivate/deactivate (\#2066).
    * Fix escaping of newlines within f-strings, in logs (\#2028).
    * Improve handling of task groups created before 2.9.0 (\#2050).
    * Add `TaskGroupCreateV2Strict` for task collections (\#2080).
    * Always create `script_dir_remote` in SSH lifecycle background tasks (\#2089).
    * Postpone setting `active=False` in task-group deactivation to after all preliminary checks (\#2100).
* Runner:
    * Improve error handling in `_zip_folder_to_file_and_remove` (\#2057).
    * Improve error handling in `FractalSlurmSSHExecutor` `handshake` method (\#2083).
    * Use the "spawn" start method for the multiprocessing context, for the `ProcessPoolExecutor`-based runner (\#2084).
    * Extract common functionalities from SLURM/sudo and SLURM/SSH executors (\#2107).
* SSH internals:
    * Add `FractalSSH.remote_exists` method (\#2008).
    * Drop `FractalSSH.{_get,_put}` wrappers of `SFTPClient` methods (\#2077).
    * Try re-opening the connection in `FractalSSH.check_connection` when an error occurs (\#2035).
    * Move `NoValidConnectionError` exception handling into `FractalSSH.log_and_raise` method (\#2070).
    * Improve closed-socket testing (\#2076).
* App:
   * Add `FRACTAL_VIEWER_AUTHORIZATION_SCHEME` and `FRACTAL_VIEWER_BASE_FOLDER` configuration variables (\#2096).
* Testing:
    * Drop `fetch-depth` from `checkout` in GitHub actions (\#2039).
* Scripts:
    * Introduce `scripts/export_v1_workflows.py` (\#2043).
* Dependencies:
    * Remove `passlib` dependency (\#2112).
    * Bump `fastapi-users` to v14, which includes switch to `pwdlib` (\#2112).

# 2.8.1

* API:
    * Validate all user-provided strings that end up in pip-install commands (\#2003).

# 2.8.0

* Task collection
    * Now both the local and SSH versions of the task collection use the bash templates (\#1980).
    * Update task-collections database logs incrementally (\#1980).
    * Add `TaskGroupV2.pinned_package_versions_string` property (\#1980).
    * Support pinned-package versions for SSH task collection (\#1980).
    * Now `pip install` uses `--no-cache` (\#1980).
* API
    * Deprecate the `verbose` query parameter in `GET /api/v2/task/collect/{state_id}/` (\#1980).
    * Add `project_dir` attribute to `UserSettings` (\#1990).
    * Set a default for `DatasetV2.zarr_dir` (\#1990).
    * Combine the `args_schema_parallel` and `args_schema_non_parallel` query parameters in `GET /api/v2/task/` into a single parameter `args_schema` (\#1998).

# 2.7.1

> WARNING: As of this version, all extras for `pip install` are deprecated and
> the corresponding dependencies become required.

* Database:
    * Drop `TaskV2.owner` column (\#1977).
    * Make `TaskV2.taskgroupv2_id` column required (\#1977).
* Dependencies:
    * Make `psycopg[binary]` dependency required, and drop `postgres-pyscopg-binary` extra (\#1970).
    * Make `gunicorn` dependency required, and drop `gunicorn` extra (\#1970).
* Testing:
    * Switch from SQLite to Postgres in the OAuth Github action (\#1981).

# 2.7.0

> WARNING: This release comes with several specific notes:
>
> 1. It requires running `fractalctl update-db-data` (after `fractalctl set-db`).
> 2. When running `fractalctl update-db-data`, the environment variable
>    `FRACTAL_V27_DEFAULT_USER_EMAIL` must be set, e.g. as in
>    `FRACTAL_V27_DEFAULT_USER_EMAIL=admin@fractal.yx fractalctl
>    update-db-data`. This user must exist, and they will own all
>    previously-common tasks/task-groups.
> 3. The pip extra `postgres` is deprecated, in favor of `postgres-psycopg-binary`.
> 4. The configuration variable `DB_ENGINE="postgres"` is deprecated, in favor of `DB_ENGINE="postgres-psycopg"`.
> 5. Python3.9 is deprecated.

* API:
    * Users and user groups:
        * Replace `UserRead.group_names` and `UserRead.group_ids` with `UserRead.group_ids_names` ordered list (\#1844, \#1850).
        * Deprecate `GET /auth/group-names/` (\#1844).
        * Add `DELETE /auth/group/{id}/` endpoint (\#1885).
        * Add `PATCH auth/group/{group_id}/user-settings/` bulk endpoint (\#1936).
    * Task groups:
        * Introduce `/api/v2/task-group/` routes (\#1817, \#1847, \#1852, \#1856, \#1943).
        * Respond with 422 error when any task-creating endpoint would break a non-duplication constraint (\#1861).
        * Enforce non-duplication constraints on `TaskGroupV2` (\#1865).
        * Fix non-duplication check in `PATCH /api/v2/task-group/{id}/` (\#1911).
        * Add cascade operations to `DELETE /api/v2/task-group/{task_group_id}/` and to `DELETE /admin/v2/task-group/{task_group_id}/` (\#1867).
        * Expand use and validators for `TaskGroupCreateV2` schema (\#1861).
        * Do not process task `source`s in task/task-group CRUD operations (\#1861).
        * Do not process task `owner`s in task/task-group CRUD operations (\#1861).
    * Tasks:
        * Drop `TaskCreateV2.source` (\#1909).
        * Drop `TaskUpdateV2.version` (\#1905).
        * Revamp access-control for `/api/v2/task/` endpoints, based on task-group attributes (\#1817).
        * Update `/api/v2/task/` endpoints and schemas with new task attributes (\#1856).
        * Forbid changing `TaskV2.name` (\#1925).
    * Task collection:
        * Improve preliminary checks in task-collection endpoints (\#1861).
        * Refactor split between task-collection endpoints and background tasks (\#1861).
        * Create `TaskGroupV2` object within task-collection endpoints (\#1861).
        * Fix response of task-collection endpoint (\#1902).
        * Automatically discover PyPI package version if missing or invalid (\#1858, \#1861, \#1902).
        * Use appropriate log-file path in collection-status endpoint (\#1902).
        * Add task `authors` to manifest schema (\#1856).
        * Do not use `source` for custom task collection (\#1893).
        * Rename custom-task-collection request-body field from `source` to `label` (\#1896).
        * Improve error messages from task collection (\#1913).
        * Forbid non-unique task names in `ManifestV2` (\#1925).
    * Workflows and workflow tasks:
        * Introduce additional checks in POST-workflowtask endpoint, concerning non-active or non-accessible tasks (\#1817).
        * Introduce additional intormation in GET-workflow endpoint, concerning non-active or non-accessible tasks (\#1817).
        * Introduce additional intormation in PATCH-workflow endpoint, concerning non-active or non-accessible tasks (\#1868, \#1869).
        * Stop logging warnings for non-common tasks in workflow export (\#1893).
        * Drop `WorkflowTaskCreateV2.order` (\#1906).
        * Update endpoints for workflow import/export  (\#1925, \#1939, \#1960).
    * Datasets:
        * Remove `TaskDumpV2.owner` attribute (\#1909).
    * Jobs:
        * Prevent job submission if includes non-active or non-accessible tasks (\#1817).
        * Remove rate limit for `POST /project/{project_id}/job/submit/` (\#1944).
    * Admin:
        * Remove `owner` from `GET admin/v2/task/` (\#1909).
        * Deprecate `kind` query parameter for `/admin/v2/task/` (\#1893).
        * Add `origin` and `pkg_name` query parameters to `GET /admin/v2/task-group/` (\#1979).
    * Schemas:
        * Forbid extras in `TaskCollectPipV2` (\#1891).
        * Forbid extras in all Create/Update/Import schemas (\#1895).
        * Deprecate internal `TaskCollectPip` schema in favor of `TaskGroupV2` (\#1861).
* Database:
    * Introduce `TaskGroupV2` table (\#1817, \#1856).
    * Add  `timestamp_created` column to `LinkUserGroup` table (\#1850).
    * Add `TaskV2` attributes `authors`, `tags`, `category` and `modality` (\#1856).
    * Add `update-db-data` script (\#1820, \#1888).
    * Add `taskgroupv2_id` foreign key to `CollectionStateV2` (\#1867).
    * Make `TaskV2.source` nullable and drop its uniqueness constraint (\#1861).
    * Add `TaskGroupV2` columns `wheel_path`, `pinned_package_versions` (\#1861).
    * Clean up `alembic` migration scripts (\#1894).
    * Verify task-group non-duplication constraint in `2.7.0` data-migration script (\#1927).
    * Normalize `pkg_name` in `2.7.0` data-migration script (\#1930).
    * Deprecate `DB_ENGINE="postgres"` configuration variable (\#1946).
* Runner:
    * Do not create local folders with 755 permissions unless `FRACTAL_BACKEND_RUNNER="slurm"` (\#1923).
    * Fix bug of SSH/SFTP commands not acquiring lock (\#1949).
    * Fix bug of unhandled exception in SSH/SLURM executor (\#1963).
    * Always remove task-subfolder compressed archive (\#1949).
* Task collection:
    * Create base directory (in SSH mode), if missing (\#1949).
    * Fix bug of SSH/SFTP commands not acquiring lock (\#1949).
* SSH:
    * Improve logging for SSH-connection-locking flow (\#1949).
    * Introduce `FractalSSH.fetch_file` and `FractalSSH.read_remote_json_file` (\#1949).
    * Use `paramiko.sftp_client.SFTPClient` methods directly rathen than `fabric` wrappers (\#1949).
    * Disable prefetching for `SFTPClient.get` (\#1949).
* Internal:
    * Update `_create_first_group` so that it only searches for `UserGroups` with a given name (\#1964).
* Dependencies:
    * Bump fastapi to `0.115` (\#1942).
    * Remove pip extra `postgres`, corresponding to `psycopg2+asyncpg` (\#1946).
    * Deprecate python3.9 (\#1946).
* Testing:
    * Benchmark `GET /api/v2/task-group/` (\#1922).
    * Use new `ubuntu22-slurm-multipy` image, with Python3.12 and with Python-version specific venvs (\#1946, #1969).
    * Get `DB_ENGINE` variable from `os.environ` rather than from installed packages (\#1968).

# 2.6.4

* Database
    * Fix use of naming convention for database schema-migration scripts (\#1819).
* Testing:
    * Test `alembic downgrade base` (\#1819).
    * Add `GET /api/v2/task/` to benchmarks (\#1825).

# 2.6.3

* API:
    * Introduce `GET /auth/current-user/viewer-paths/` endpoint (\#1816).
    * Add `viewer_paths` attribute to `UserGroup` endpoints (\#1816).
* Database:
    * Add  `viewer_paths` column to `UserGroup` table (\#1816).
* Runner:
    * Anticipate `wait_thread.shutdown_callback` assignment in `FractalSlurmExecutor`, to avoid an uncaught exception (\#1815).

# 2.6.2

* Allow setting `UserSettings` attributes to `None` in standard/strict PATCH endpoints (\#1814).

# 2.6.1

* App (internal):
    * Remove `FRACTAL_SLURM_SSH_HOST`, `FRACTAL_SLURM_SSH_USER`, `FRACTAL_SLURM_SSH_PRIVATE_KEY_PATH` and `FRACTAL_SLURM_SSH_WORKING_BASE_DIR` from `Settings`  (\#1804).
* Database:
    * Drop `slurm_user`, `slurm_accounts` and `cache_dir` columns from `UserOAuth` (\#1804)

# 2.6.0

> WARNING: This release requires running `fractalctl update-db-data` (after `fractalctl set-db`).

* API:
    * Introduce user-settings API, in `/auth/users/{user_id}/settings/` and `/auth/current-user/settings/` (\#1778, \#1807).
    * Add the creation of empty settings to `UserManager.on_after_register` hook (\#1778).
    * Remove deprecated user's attributes (`slurm_user`, `cache_dir`, `slurm_accounts`) from API, in favor of new `UserSetting` ones (\#1778).
    * Validate user settings in endpoints that rely on them (\#1778).
    * Propagate user settings to background tasks when needed (\#1778).
* Database:
    * Introduce new `user_settings` table, and link it to `user_oauth` (\#1778).
* Internal:
   * Remove redundant string validation in `FractalSSH.remove_folder` and `TaskCollectCustomV2` (\#1810).
   * Make `validate_cmd` more strict about non-string arguments (\#1810).


# 2.5.2

* App:
    * Replace `fractal_ssh` attribute with `fractal_ssh_list`, in `app.state` (\#1790).
    * Move creation of SSH connections from app startup to endpoints (\#1790).
* Internal
    * Introduce `FractalSSHList`, in view of support for multiple SSH/Slurm service users (\#1790).
    * Make `FractalSSH.close()` more aggressively close `Transport` attribute (\#1790).
    * Set `look_for_keys=False` for paramiko/fabric connection (\#1790).
* Testing:
    * Add fixture to always test that threads do not accumulate during tests (\#1790).

# 2.5.1

* API:
    * Make `WorkflowTaskDumpV2` attributes `task_id` and `task` optional (\#1784).
    * Add validation for user-provided strings that execute commands with subprocess or remote-shell (\#1767).
* Runner and task collection:
    * Validate commands before running them via `subprocess` or `fabric` (\#1767).

# 2.5.0

> WARNING: This release has a minor API bug when displaying a V2 dataset with a history that contains legacy tasks. It's recommended to update to 2.5.1.

This release removes support for including V1 tasks in V2 workflows. This comes
with changes to the database (data and metadata), to the API, and to the V2
runner.

* Runner:
    * Deprecate running v1 tasks within v2 workflows (\#1721).
* Database:
    * Remove `Task.is_v2_compatible` column (\#1721).
    * For table `WorkflowTaskV2`, drop `is_legacy_task` and `task_legacy_id` columns, remove `task_legacy` ORM attribute, make `task_id` required, make `task` required (\#1721).
* API:
    * Drop v1-v2-task-compatibility admin endpoint (\#1721).
    * Drop `/task-legacy/` endpoint (\#1721).
    * Remove legacy task code branches from `WorkflowTaskV2` CRUD endpoints (\#1721).
    * Add OAuth accounts info to `UserRead` at `.oauth_accounts` (\#1765).
* Testing:
    * Improve OAuth Github Action to test OAuth account flow (\#1765).

# 2.4.2

* App:
    * Improve logging in `fractalctl set-db` (\#1764).
* Runner:
    * Add `--set-home` to `sudo -u` impersonation command, to fix Ubuntu18 behavior (\#1762).
* Testing:
    * Start tests of migrations from valid v2.4.0 database (\#1764).

# 2.4.1

This is mainly a bugfix release, re-implementing a check that was removed in 2.4.0.

* API:
    * Re-introduce check for existing-user-email in `PATCH /auth/users/{id}/` (\#1760).

# 2.4.0

This release introduces support for user groups, but without linking it to any
access-control rules (which will be introduced later).

> NOTE: This release requires running the `fractalctl update-db-data` script.

* App:
    * Move creation of first user from application startup into `fractalctl set-db` command (\#1738, \#1748).
    * Add creation of default user group into `fractalctl set-db` command (\#1738).
    * Create `update-db-script` for current version, that adds all users to default group (\#1738).
* API:
    * Added `/auth/group/` and `/auth/group-names/` routers (\#1738, \#1752).
    * Implement `/auth/users/{id}/` POST/PATCH routes in `fractal-server` (\#1738, \#1747, \#1752).
    * Introduce `UserUpdateWithNewGroupIds` schema for `PATCH /auth/users/{id}/` (\#1747, \#1752).
    * Add `UserManager.on_after_register` hook to add new users to default user group (\#1738).
* Database:
    * Added new `usergroup` and `linkusergroup` tables (\#1738).
* Internal
    * Refactored `fractal_server.app.auth` and `fractal_server.app.security` (\#1738)/
    * Export all relevant modules in `app.models`, since it matters e.g. for `autogenerate`-ing migration scripts (\#1738).
* Testing
    * Add `UserGroup` validation to `scripts/validate_db_data_with_read_schemas.py` (\#1746).


# 2.3.11

* SSH runner:
    * Move remote-folder creation from `submit_workflow` to more specific `_process_workflow` (\#1728).
* Benchmarks:
    * Add `GET /auth/token/login/` to tested endpoints (\#1720).
* Testing:
    * Update GitHub actions `upload-artifact` and `download-artifact` to `v4` (\#1725).

# 2.3.10

* Fix minor bug in zipping-job logging (\#1716).

# 2.3.9

* Add logging for zipping-job-folder operations (\#1714).

# 2.3.8

> NOTE: `FRACTAL_API_V1_MODE="include_without_submission"` is now transformed
> into `FRACTAL_API_V1_MODE="include_read_only"`.

* API:
    * Support read-only mode for V1 (\#1701).
    * Improve handling of zipped job-folder in download-logs endpoints (\#1702).
* Runner:
    * Improve database-error handling in V2 job execution (\#1702).
    * Zip job folder after job execution (\#1702).
* App:
    * `UvicornWorker` is now imported from `uvicorn-worker` (\#1690).
* Testing:
    * Remove `HAS_LOCAL_SBATCH` variable and related if-branches (\#1699).
* Benchmarks:
    * Add `GET /auth/current-user/` to tested endpoints (\#1700).
* Dependencies:
    * Update `mkdocstrings` to `^0.25.2` (\#1707).
    * Update `fastapi` to `^0.112.0` (\#1705).

# 2.3.7

* SSH SLURM executor:
    * Handle early shutdown in SSH executor (\#1696).
* Task collection:
    * Introduce a new configuration variable `FRACTAL_MAX_PIP_VERSION` to pin task-collection pip (\#1675).

# 2.3.6

* API:
    * When creating a WorkflowTask, do not pre-populate its top-level arguments based on JSON Schema default values (\#1688).
* Dependencies:
    * Update `sqlmodel` to `^0.0.21` (\#1674).
    * Add `uvicorn-worker` (\#1690).

# 2.3.5

> WARNING: The `pre_submission_commands` SLURM configuration is included as an
> experimental feature, since it is still not useful for its main intended
> goal (calling `module load` before running `sbatch`).

* SLURM runners:
    * Expose `gpus` SLURM parameter (\#1678).
    * For SSH executor, add `pre_submission_commands` (\#1678).
    * Removed obsolete arguments from `get_slurm_config` function (\#1678).
* SSH features:
    * Add `FractalSSH.write_remote_file` method (\#1678).


# 2.3.4

* SSH SLURM runner:
    * Refactor `compress_folder` and `extract_archive` modules, and stop using `tarfile` library (\#1641).
* API:
    * Introduce `FRACTAL_API_V1_MODE=include_without_submission` to include V1 API but forbid job submission (\#1664).
* Testing:
    * Do not test V1 API with `DB_ENGINE="postgres-psycopg"` (\#1667).
    * Use new Fractal SLURM containers in CI (\#1663).
    * Adapt tests so that they always refer to the current Python version (the one running `pytest`), when needed; this means that we don't require the presence of any additional Python version in the development environment, apart from the current one (\#1633).
    * Include Python3.11 in some tests (\#1669).
    * Simplify CI SLURM Dockerfile after base-image updates (\#1670).
    * Cache `ubuntu22-slurm-multipy` Docker image in CI (\#1671).
    * Add `oauth.yaml` GitHub action to test OIDC authentication (\#1665).

# 2.3.3

This release fixes a SSH-task-collection bug introduced in version 2.3.1.

* API:
    * Expose new superuser-restricted endpoint `GET /api/settings/` (\#1662).
* SLURM runner:
    * Make `FRACTAL_SLURM_SBATCH_SLEEP` configuration variable `float` (\#1658).
* SSH features:
    * Fix wrong removal of task-package folder upon task-collection failure (\#1649).
    * Remove `FractalSSH.rename_folder` method (\#1654).
* Testing:
    * Refactor task-collection fixtures (\#1637).

# 2.3.2

> **WARNING**: The remove-remote-venv-folder in the SSH task collection is broken (see issue 1633). Do not deploy this version in an SSH-based `fractal-server` instance.

* API:
    * Fix incorrect zipping of structured job-log folders (\#1648).

# 2.3.1

This release includes a bugfix for task names with special characters.

> **WARNING**: The remove-remote-venv-folder in the SSH task collection is broken (see issue 1633). Do not deploy this version in an SSH-based `fractal-server` instance.

* Runner:
    * Improve sanitization of subfolder names (commits from 3d89d6ba104d1c6f11812bc9de5cbdff25f81aa2 to 426fa3522cf2eef90d8bd2da3b2b8a5b646b9bf4).
* API:
    * Improve error message when task-collection Python is not defined (\#1640).
    * Use a single endpoint for standard and SSH task collection (\#1640).
* SSH features:
    * Remove remote venv folder upon failed task collection in SSH mode (\#1634, \#1640).
    * Refactor `FractalSSH` (\#1635).
    * Set `fabric.Connection.forward_agent=False` (\#1639).
* Testing:
    * Improved testing of SSH task-collection API (\#1640).
    * Improved testing of `FractalSSH` methods (\#1635).
    * Stop testing SQLite database for V1 in CI (\#1630).

# 2.3.0

This release includes two important updates:
1. An Update update to task-collection configuration variables and logic.
2. The first released version of the **experimental** SSH features.

Re: task-collection configuration, we now support two main use cases:

1. When running a production instance (including on a SLURM cluster), you
   should set e.g. `FRACTAL_TASKS_PYTHON_DEFAULT_VERSION=3.10`, and make sure
   that `FRACTAL_TASKS_PYTHON_3_10=/some/python` is an absolute path. Optionally,
   you can define other variables like `FRACTAL_TASKS_PYTHON_3_9`,
   `FRACTAL_TASKS_PYTHON_3_11` or `FRACTAL_TASKS_PYTHON_3_12`.

2. If you leave `FRACTAL_TASKS_PYTHON_DEFAULT_VERSION` unset, then only the
   Python interpreter that is currently running `fractal-server` can be used
   for task collection.

> WARNING: If you don't set `FRACTAL_TASKS_PYTHON_DEFAULT_VERSION`, then you
> will only have a single Python interpreter available for tasks (namely the
> one running `fractal-server`).

* API:
    * Introduce `api/v2/task/collect/custom/` endpoint (\#1607, \#1613, \#1617, \#1629).
* Task collection:
    * Introduce task-collection Python-related configuration variables (\#1587).
    * Always set Python version for task collection, and only use `FRACTAL_TASKS_PYTHON_X_Y` variables (\#1587).
    * Refactor task-collection functions and schemas (\#1587, \#1617).
    * Remove `TaskCollectStatusV2` and `get_collection_data` internal schema/function (\#1598).
    * Introduce `CollectionStatusV2` enum for task-collection status (\#1598).
    * Reject task-collection request if it includes a wheel file and a version (\#1608).
SSH features:
    * Introduce `fractal_server/ssh` subpackage (\#1545, \#1599, \#1611).
    * Introduce SSH executor and runner (\#1545).
    * Introduce SSH task collection (\#1545, \#1599, \#1626).
    * Introduce SSH-related configuration variables (\#1545).
    * Modify app lifespan to handle SSH connection (\#1545).
    * Split `app/runner/executor/slurm` into `sudo` and `ssh` subfolders (\#1545).
    * Introduce FractalSSH object which is a wrapper class around fabric.Connection object.
It provides a `lock` to avoid loss of ssh instructions and a custom timeout (\#1618)
* Dependencies:
    * Update `sqlmodel` to `^0.0.19` (\#1584).
    * Update `pytest-asyncio` to `^0.23` (\#1558).
* Testing:
    * Test the way `FractalProcessPoolExecutor` spawns processes and threads (\#1579).
    * Remove `event_loop` fixture: every test will run on its own event loop (\#1558).
    * Test task collection with non-canonical package name (\#1602).

# 2.2.0

This release streamlines options for the Gunicorn startup command, and includes
two new experimental features.

> NOTE 1: you can now enable custom Gunicorn worker/logger by adding the following
> options to the `gunicorn` startup command:
> - `--worker-class fractal_server.gunicorn_fractal.FractalWorker`
> - `--logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger`

> NOTE 2: A new experimental local runner is available, which uses processes
> instead of threads and support shutdown. You can try it out with the
> configuration variable `FRACTAL_BACKEND_RUNNER=local_experimental`

> NOTE 3: A new PostgreSQL database adapter is available, fully based on
> `psycopg3` (rather than `pyscopg2`+`asyncpg`). You can try it out with the
> configuration variable `DB_ENGINE=postgres-psycopg` (note that this requires
> the `pip install` extra `postgres-psycopg-binary`).


* API:
    * Add extensive logs to `DELETE /api/v2/project/{project_id}` (\#1532).
    * Remove catch of `IntegrityError` in `POST /api/v1/project` (\#1530).
* App and deployment:
    * Move `FractalGunicornLogger` and `FractalWorker` into `fractal_server/gunicorn_fractal.py` (\#1535).
    * Add custom gunicorn/uvicorn worker to handle SIGABRT signal (\#1526).
    * Store list of submitted jobs in app state (\#1538).
    * Add logic for graceful shutdown for job slurm executors (\#1547).
* Runner:
    * Change structure of job folders, introducing per-task subfolders (\#1523).
    * Rename internal `workflow_dir` and `workflow_dir_user` variables to local/remote (\#1534).
    * Improve handling of errors in `submit_workflow` background task (\#1556, \#1566).
    * Add new `local_experimental` runner, based on `ProcessPoolExecutor` (\#1544, \#1566).
* Database:
    * Add new Postgres adapter `psycopg` (\#1562).
* Dependencies
    * Add `fabric` to `dev` dependencies (\#1518).
    * Add new `postgres-psycopg-binary` extra (\#1562).
* Testing:
    * Extract `pytest-docker` fixtures into a dedicated module (\#1516).
    * Rename SLURM containers in CI (\#1516).
    * Install and run SSH daemon in CI containers (\#1518).
    * Add unit test of SSH connection via fabric/paramiko (\#1518).
    * Remove obsolete folders from `tests/data` (\#1517).

# 2.1.0

This release fixes a severe bug where SLURM-executor auxiliary threads are
not joined when a Fractal job ends.

* App:
    * Add missing join for `wait_thread` upon `FractalSlurmExecutor` exit (\#1511).
    * Replace `startup`/`shutdown` events with `lifespan` event (\#1501).
* API:
    * Remove `Path.resolve` from the submit-job endpoints and add validator for `Settings.FRACTAL_RUNNER_WORKING_BASE_DIR` (\#1497).
* Testing:
    * Improve dockerfiles for SLURM (\#1495, \#1496).
    * Set short timeout for `docker compose down` (\#1500).

# 2.0.6

> NOTE: This version changes log formats.
> For `uvicorn` logs, this change requires no action.
> For `gunicorn`, logs formats are only changed by adding the following
> command-line option:
> `gunicorn ... --logger-class fractal_server.logger.gunicorn_logger.FractalGunicornLogger`.

* API:
    * Add `FRACTAL_API_V1_MODE` environment variable to include/exclude V1 API (\#1480).
    * Change format of uvicorn loggers (\#1491).
    * Introduce `FractalGunicornLogger` class (\#1491).
* Runner:
    * Fix missing `.log` files in server folder for SLURM jobs (\#1479).
* Database:
    * Remove `UserOAuth.project_list` and `UserOAuth.project_list_v2` relationships (\#1482).
* Dev dependencies:
    * Bump `pytest` to `8.1.*` (#1486).
    * Bump `coverage` to `7.5.*` (#1486).
    * Bump `pytest-docker` to `3.1.*` (#1486).
    * Bump `pytest-subprocess` to `^1.5` (#1486).
* Benchmarks:
    * Move `populate_db` scripts into `benchmark` folder (\#1489).


# 2.0.5

* API:
    * Add `GET /admin/v2/task/` (\#1465).
    * Improve error message in DELETE-task endpoint (\#1471).
* Set `JobV2` folder attributes from within the submit-job endpoint (\#1464).
* Tests:
    * Make SLURM CI work on MacOS (\#1476).

# 2.0.4

* Add `FRACTAL_SLURM_SBATCH_SLEEP` configuration variable (\#1467).

# 2.0.3

> WARNING: This update requires running a fix-db script, via `fractalctl update-db-data`.

* Database:
    * Create fix-db script to remove `images` and `history` from dataset dumps in V1/V2 jobs (\#1456).
* Tests:
    * Split `test_full_workflow_v2.py` into local/slurm files (\#1454).


# 2.0.2

> WARNING: Running this version on a pre-existing database (where the `jobsv2`
> table has some entries) is broken. Running this version on a freshly-created
> database works as expected.

* API:
    * Fix bug in status endpoint (\#1449).
    * Improve handling of out-of-scope scenario in status endpoint (\#1449).
    * Do not include dataset `history` in `JobV2.dataset_dump` (\#1445).
    * Forbid extra arguments in `DumpV2` schemas (\#1445).
* API V1:
    * Do not include dataset `history` in `ApplyWorkflow.{input,output}_dataset_dump` (\#1453).
* Move settings logs to `check_settings` and use fractal-server `set_logger` (\#1452).
* Benchmarks:
    * Handle some more errors in benchmark flow (\#1445).
* Tests:
    * Update testing database to version 2.0.1 (\#1445).

# 2.0.1

* Database/API:
    * Do not include `dataset_dump.images` in `JobV2` table (\#1441).
* Internal functions:
    * Introduce more robust `reset_logger_handlers` function (\#1425).
* Benchmarks:
    * Add `POST /api/v2/project/project_id/dataset/dataset_id/images/query/` in bechmarks  to evaluate the impact of the number of images during the query (\#1441).
* Development:
    * Use `poetry` 1.8.2 in GitHub actions and documentation.

# 2.0.0

Major update.

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
    * Make `WorkflowTask.workflow_id` and `WorkflowTask.task_id` not nullable (\#1137).
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
* Update endpoints (see also [1.2->1.3 upgrade info](internals/version_upgrades/upgrade_1_2_5_to_1_3_0.md) in the documentation):
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

* Review setup for database URLs, especially to allow using UNIX-socket connections for postgresql (\#657).

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
