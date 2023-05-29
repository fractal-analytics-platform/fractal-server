**Note**: Numbers like (\#123) point to closed Pull Requests on the fractal-server repository.

# 1.3.0

* Refactor user model:
    * Switch from UUID4 to int for IDs (\#660, \#684).
    * Fix many-to-many relationship between users and project (\#660).
    * Rename `Project.user_member_list` into `Project.user_list` (\#660).
    * Add `username` column (\#704).
* Update endpoints (see also [1.2->1.3 upgrade info](../internals/version_upgrades/upgrade_1_2_5_to_1_3_0/) in the documentation):
    * Review endpoint URLs (\#669).
    * Remove foreign keys from payloads (\#669).
* Update `Task` models, task collection and task-related endpoints (\#704):
    * Add `version` and `owner` columns to `Task` model (\#704).
    * Set `Task.version` during task collection (\#719).
    * Set `Task.owner` as part of create-task endpoint (\#704).
    * For custom tasks, prepend `owner` to user-provided `source` (\#725).
    * Make `Task.source` task-specific rather than package-specific (\#719).
    * Make `Task.source` unique (\#725).
    * When importing a workflow, only use tasks' `source` values, instead of `(source,name)` pairs (\#719).
    * Update `_TaskCollectPip` methods, attributes and properties (\#719).
    * Remove private/public options for task collection (\#704).
    * Improve error message for missing package manifest (\#704).
    * Improve behavior when task-collection folder already exists (\#704).
    * Add warning when exporting workflows which include custom tasks (\#728).
* Job execution:
    * Add `FractalSlurmExecutor.shutdown` and corresponding endpoint (\#631, \#691, \#696).
    * In `FractalSlurmExecutor`, make `working_dir*` attributes required (\#679).
    * Remove `ApplyWorkflow.overwrite_input` column (\#684, \#694).
    * Make `output_dataset_id` a required argument of apply-workflow endpoint (\#681).
    * Improve error message related to out-of-space disk (\#699).
* Other updates to endpoints and database:
    * Add `ApplyWorkflow.end_timestamp` column (\#687, \#684).
    * Prevent deletion of a `Workflow`/`Dataset` in relationship with existing `ApplyWorkflow` (\#703).
    * Add project-name uniqueness constraint in project-edit endpoint (\#689).
* Expose `FRACTAL_CORS_ALLOW_ORIGIN` environment variable (\#688).
* Package and repository:
    * Remove `fastapi-users-db-sqlmodel` dependency (\#660).
    * Make coverage measure more accurate (\#676) and improve coverage (\#678).
    * Require pydantic version to be `>=1.10.8` (\#711, \#713).
    * Include multiple `fractal-common` updates (\#705, \#719).
    * Add test equivalent to `alembic check` (\#722).
    * Update `poetry.lock` to address security alerts (\#723).
    * Remove `sqlmodel` from `fractal-common`, and declare database models with multiple inheritance (\#710).
    * Make email generation more robust in `MockCurrentUser` (\#730).

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
