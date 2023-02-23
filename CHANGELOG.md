**Note**: Numbers like (\#123) point to closed Pull Requests on the fractal-server repository.

# Unreleased

* Drop python 3.8 support (\#527).

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
