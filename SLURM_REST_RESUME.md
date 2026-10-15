# SLURM REST runner and resume of orphaned jobs (temporary notes)

> Temporary file describing the changes on branch `feat/slurm_api_runner_resumable` (based on v2.24.2).

The branch adds two things:

1. **SLURM REST runner**: submits, polls and cancels SLURM jobs through `slurmrestd` (HTTPS) instead of `sbatch`/`squeue`/`scancel` over SSH. Goal is to aim for user-based submissions.
2. **Resume of orphaned jobs**: if fractal-server is killed hard (`kill -9`, OOM, SIGKILL of the container), jobs stay `submitted` forever. Admin endpoints now detect them and either close them (`done`/`failed`) or restart the workflow from the interrupted task, adopting SLURM jobs that are still running or already finished.

**Guiding principle: touch the existing codebase as little as possible**, so that a future merge with upstream stays easy and does not change the behaviour of existing features. In practice:

- New code lives in new modules (`runner/executors/slurm_rest/`, `runner/v2/_slurm_rest.py`, `runner/v2/resume/`, `app/routes/admin/v2/resume.py`), and new tests in new test files.
- The REST runner subclasses the SSH runner and overrides only the transport (submit, status query, cancel), reusing all the existing job logic.
- New settings are in a separate settings class: the main `Settings` and its validation are unchanged.
- Everything is off by default (`FRACTAL_SLURM_REST_ENABLED=false`): without it, existing deployments behave exactly as before.
- No database schema changes and no migrations; resume state is kept in the existing `job.log`.
- Only four existing files are changed:
  - [admin/v2/\_\_init\_\_.py](fractal_server/app/routes/admin/v2/__init__.py): two lines to register the resume router.
  - [gunicorn_fractal.py](fractal_server/gunicorn_fractal.py): two lines that store the server start time in an environment variable, see [Server start time](#server-start-time).
  - [submit_workflow.py](fractal_server/runner/v2/submit_workflow.py): dispatch to the REST runner when enabled, and an optional `resume=` argument (default `None` = unchanged behaviour) that sets the start task, type filters and adopt mode, reuses the existing local job folder, and adds resume lines to the logs.
  - [runner.py](fractal_server/runner/v2/runner.py): the end-of-task results are saved in one commit instead of four (see [Engine change](#engine-change-upstream-code)); this is the only change to shared engine code. This simplifies the reconciliation effort, but workaround exists if the change is anyhow problematic can be removed.

## 1. SLURM REST runner

### Enabling it

`FRACTAL_RUNNER_BACKEND` stays `slurm_ssh`; the REST runner is switched on by an extra setting, for **all** `slurm_ssh` resources (read for each job, no restart needed). There is no fallback to SSH.


TEMPORARY SETTING STRUCTURE:
| Variable | Default | Meaning |
|---|---|---|
| `FRACTAL_SLURM_REST_ENABLED` | `false` | `true` to use the REST runner |
| `FRACTAL_SLURM_REST_URL` | – | Base URL of `slurmrestd` (without `/slurm/<version>`); required if enabled |
| `FRACTAL_SLURM_REST_API_VERSION` | `v0.0.44` | OpenAPI plugin version |
| `FRACTAL_SLURM_REST_VERIFY_TLS` | `true` | Verify the TLS certificate of `slurmrestd` |
| `FRACTAL_SLURM_REST_CA_BUNDLE` | – | PEM file with the CAs to trust (e.g. an internal CA); if set, it replaces the system CAs |
| `FRACTAL_RESUME_ON_STARTUP` | `false` | Resume orphaned jobs automatically at server start, see [Startup hook](#startup-hook) |

These live in their own settings class ([settings.py](fractal_server/runner/executors/slurm_rest/settings.py)), so the main `Settings` is unchanged. URL, API version and CA bundle are per cluster and should move to the `Resource` (TODO in the code); as global settings, all `slurm_ssh` resources share them. Fixed in the code: HTTP timeout 30 s ([client.py](fractal_server/runner/executors/slurm_rest/client.py)), token lifespan 3600 s ([token.py](fractal_server/runner/executors/slurm_rest/token.py)), 20 consecutive failed status queries before giving up ([runner.py](fractal_server/runner/executors/slurm_rest/runner.py)).

### How it works

- **Authentication**: the server runs `scontrol token lifespan=...` over SSH (as the profile user), keeps the token in memory only (never logged), and sends it in the `X-SLURM-USER-NAME` / `X-SLURM-USER-TOKEN` headers. It is refreshed at 80% of its lifespan, or once on HTTP 401/511. --> TEMPORARY UNTIL FIGURING OUT PROPER SSO AUTH
- **Submission**: the `#SBATCH` lines are converted to the JSON job description; an unknown option is an error. If `slurmrestd` warns about an unknown field, the job is cancelled and the submission fails.
- **No upload**: the job script itself writes its input/args JSON files (quoted heredocs) and a copy of itself (`cp "$0"`) in the remote folder. Limit: 4 MB per script.
- **SSH is still needed** for: connection and worker-version checks, `mkdir`, `printenv HOME` and `printenv PATH` (the job gets the same `PATH` as with `sbatch` over SSH), (the token), and fetching outputs (file list + `tar` + SFTP), as in the SSH runner.
- **Identification**: every SLURM job is found by its stdout path `{job.working_dir_user}/{subfolder}/{prefix}-slurm-%j.out` from the API (SLURM stores for every job).

| REST call | When |
|---|---|
| `GET /slurm/<v>/ping` | Runner start |
| `POST /slurm/<v>/job/submit` | Each batch |
| `GET /slurm/<v>/jobs` | Each poll |
| `DELETE /slurm/<v>/job/{id}` | Stop request, or unknown-field warning |
| `GET /slurmdb/<v>/jobs` | Resume only (finished jobs) |

### Files

| File | Content |
|---|---|
| [slurm_rest/client.py](fractal_server/runner/executors/slurm_rest/client.py) | Minimal HTTP client (stdlib `urllib`) |
| [slurm_rest/token.py](fractal_server/runner/executors/slurm_rest/token.py) | `scontrol token` over SSH, in-memory cache |
| [slurm_rest/job_description.py](fractal_server/runner/executors/slurm_rest/job_description.py) | `#SBATCH` lines -> JSON fields |
| [slurm_rest/script.py](fractal_server/runner/executors/slurm_rest/script.py) | Self-contained job script |
| [slurm_rest/runner.py](fractal_server/runner/executors/slurm_rest/runner.py) | `SlurmRestRunner(SlurmSSHRunner)`: overrides submit, status query, cancel; adopt mode for resume |
| [runner/v2/_slurm_rest.py](fractal_server/runner/v2/_slurm_rest.py) | `process_workflow` with `SlurmRestRunner` (copy of `_slurm_ssh.py`) |
| [runner/v2/submit_workflow.py](fractal_server/runner/v2/submit_workflow.py) | Dispatch to the REST runner when enabled; `resume=` argument |

## 2. Resume of orphaned jobs

### Startup hook

With `FRACTAL_RESUME_ON_STARTUP=true` (and backend `slurm_ssh`), every server start does, in the background, what the endpoints below do by hand: for each `submitted` job started before the [server start](#server-start-time), build its plan and apply it (`resume`, `mark_done` or `mark_failed`). Refused plans are only logged (`Startup resume: ...` lines), for an admin to check with the endpoints.

- It is the `lifespan` of the resume router ([resume.py](fractal_server/app/routes/admin/v2/resume.py)); FastAPI runs it after the app startup in `main.py`, which is unchanged. No API token is needed.
- It runs in a background task: the server answers requests at once, and a slow SSH/SLURM call cannot delay the startup.
- Every gunicorn worker runs it; the job-row lock and the resume marker (same server start time in all workers) let only one worker apply each plan, the others log `skipped`.
- Errors are logged per job and never raised. A crash at any point leaves the job `submitted`, and the next start plans it again.
- Resumed jobs are added to `app.state.jobs`, so the graceful shutdown handles them like any other job.

### Endpoints (superuser only)

| Call | What it does | Changes anything? |
|---|---|---|
| `GET /admin/v2/resume/orphans/` | Plan for every `submitted` job started before the server start | No |
| `GET /admin/v2/resume/job/{id}/` | Plan for one job (no start-time cutoff) | No |
| `POST /admin/v2/resume/job/{id}/` | Apply the plan: mark done/failed, or relaunch | Yes |

Query parameters: `started_before` (orphans only, default: server start), `check_remote=false` (GETs only, skip SSH/SLURM checks), `force=true` (POST only, accept jobs started after the server start).

### Server start time

The default cutoff and the resume marker use the start time of the **gunicorn master**, so all workers share it. Gunicorn imports `fractal_server.gunicorn_fractal` in the master (through `--logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger`), which sets `FRACTAL_SERVER_BOOT_TIMESTAMP` in the environment; the workers, forked later (also when one is restarted), inherit it. Without gunicorn (e.g. `fractalctl start`), it is the start of the single server process.

```bash
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/admin/v2/resume/orphans/" | jq
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/admin/v2/resume/job/42/" | jq
curl -s -X POST -H "Authorization: Bearer $TOKEN" "$BASE/admin/v2/resume/job/42/" | jq
```

Only jobs on `slurm_ssh` resources with `FRACTAL_SLURM_REST_ENABLED=true` are considered.

### Cases

The decision uses the database only; SLURM is queried for safety checks and to list the jobs to adopt. `k` = first task of the job range without a `done` task run.

| Case | Database state | Action |
|---|---|---|
| C0 | `working_dir` not set (crash inside `submit_job`) | `mark_failed` |
| C1 | No task runs | `resume` from the first task |
| C2 | All tasks `done` | `mark_done` |
| C3 | A task run `failed` | `mark_failed` |
| C4 | Task k not started | `resume` from k |
| C5 | Task k run without units | delete the run, `resume` from k |
| C6 | Task k run with units (finished or not) | delete the run, `resume` from k, adopt SLURM jobs |

The plan is refused (nothing changes) if, among others: project/workflow/dataset was deleted, task k changed since it started, the image selection would differ, SLURM jobs exist for later tasks, or any SSH/SLURM call fails.

### Apply and adoption

- `POST` locks the job row (`FOR UPDATE SKIP LOCKED`), writes a marker `RESUME boot=<server start> k=<k> <case> -> <action>` in `job.log` (blocks a second resume by the same server, also from another worker, and lets an interrupted resume be recognised), and commits once. Mark-only cases then zip the job folder as a normal job end.
- The relaunched `submit_workflow` starts at task k with the replayed type filters. For each batch of task k, the runner adopts the existing SLURM job if its script is identical (pending/running: keep polling; finished: fetch outputs); submits it if missing; cancels and resubmits it if its script cannot be read.

### Engine change (upstream code)

In `execute_tasks` ([runner/v2/runner.py](fractal_server/runner/v2/runner.py)), the end of each task now commits the executor error log first, then dataset images, image-cache cleanup, accounting record and task-run status **in one commit** (upstream: four commits). Final states are unchanged; a crash in the middle now leaves nothing saved, so an open task run always means "results not saved". This is what makes C6 safe, otherwise sub-cases open based on petential crash points and need to be handled.

### Files

| File | Content |
|---|---|
| [resume/plan.py](fractal_server/runner/v2/resume/plan.py) | Read-only planning (cases, checks) |
| [resume/slurm_evidence.py](fractal_server/runner/v2/resume/slurm_evidence.py) | SLURM jobs of a Fractal job, by stdout path |
| [resume/actions.py](fractal_server/runner/v2/resume/actions.py) | Apply a plan (single transaction) |
| [resume/spec.py](fractal_server/runner/v2/resume/spec.py) | `ResumeSpec` passed to `submit_workflow` |
| [admin/v2/resume.py](fractal_server/app/routes/admin/v2/resume.py) | Endpoints and startup hook |

## 3. Tests

In `tests/v2/test_08_backends/`:

- `test_slurm_rest_unit.py`, `test_slurm_rest_adopt_unit.py`, `test_resume_plan_unit.py`, `test_resume_startup_unit.py`: unit tests, no cluster needed.
- `test_slurm_rest_runner.py`: needs the SLURM docker containers (fake REST client that runs `sbatch` over SSH).

## 4. Known limitations / open points

- The startup hook is off by default and not yet tested on a real deployment.
- If a single gunicorn worker dies while the server keeps running, its jobs started after the server start, so they are not listed as orphans. They can be resumed with `POST ...?force=true`, but only once you are sure no live worker runs them (the "running here" check only sees the worker that answers the request).
- The shared start time relies on `--logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger`; without it, each worker uses its own start time.
- No limit on the number of resumes of the same job (see `TODO(review)` in `actions.py`).
- REST outage longer than `MAX_POLL_FAILURES` is not handled specifically.
- Some `#SBATCH` -> JSON mappings (`gres`, `gpus`, `exclude`, `nodelist`, `nodes`, `qos`) are not verified on a real cluster.
