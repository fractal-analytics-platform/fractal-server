# Resume of orphaned jobs

If `fractal-server` is killed hard (`kill -9`, OOM killer, `SIGKILL` of the container), its jobs stay `submitted` forever. The resume feature detects these _orphaned_ jobs and either closes them (`done`/`failed`) or restarts the workflow from the interrupted task, adopting the SLURM jobs that are still running or already finished.

Resume is only available for runners that support it (`BaseRunner.supports_resume`); today this is only the [SLURM REST runner](./slurm_rest.md). Jobs of other runners are never touched.

## Endpoints (superuser only)

| Call | What it does | Changes anything? |
|---|---|---|
| `GET /admin/v2/resume/orphans/` | Plan for every `submitted` job started before the server start | No |
| `GET /admin/v2/resume/job/{id}/` | Plan for one job (no start-time cutoff) | No |
| `POST /admin/v2/resume/job/{id}/` | Apply the plan: mark done/failed, or relaunch | Yes |

Query parameters:

- `started_before` (`GET .../orphans/` only): cutoff, default is the server start;
- `check_remote=false` (`GET` only): skip the SSH/SLURM checks;
- `force=true` (`POST` only): also accept a job started after the server start.

## Startup resume

With `FRACTAL_RESUME_ON_STARTUP=true` (see [`ResumeSettings`](../../configuration.md#fractal_server.runner.v2.resume.settings.ResumeSettings)), every server start does in the background what the endpoints do: for each `submitted` job started before the server start, build its plan and apply it. Refused plans are only logged (`Startup resume: ...` lines), for an admin to check with the endpoints.

- It runs in a background task, so a slow SSH or SLURM call does not delay the startup. Errors are logged per job and never raised; a crash at any point leaves the job `submitted`, and the next start plans it again.
- Every gunicorn worker starts it, but only the first one that creates a lock file (`fractal-resume-<server start>.lock` in the temporary folder) runs it; the others log `run by another worker`.
- Resumed jobs are added to `app.state.jobs`, so the graceful shutdown handles them like any other job.

### Server start time

The default cutoff and the resume marker use the start time of the **gunicorn master**, so that all workers share it. Gunicorn imports `fractal_server.gunicorn_fractal` in the master (through `--logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger`), which sets `FRACTAL_SERVER_BOOT_TIMESTAMP` in the environment; workers, forked later, inherit it. Without gunicorn, it is the start of the single server process.

## Cases

The decision uses the database only; SLURM is queried for safety checks and to list the SLURM jobs to adopt. `k` is the first task of the job range without a `done` task run.

| Case | Database state | Action |
|---|---|---|
| C0 | `working_dir` not set (crash inside the job submission) | `mark_failed` |
| C1 | No task runs | `resume` from the first task |
| C2 | All tasks `done` | `mark_done` |
| C3 | A task run `failed` | `mark_failed` |
| C4 | Task k not started | `resume` from k |
| C5 | Task k run without units | delete the run, `resume` from k |
| C6 | Task k run with units (finished or not) | delete the run, `resume` from k, adopt SLURM jobs |

The plan is refused (nothing changes) if, among others: the project, workflow or dataset was deleted, task k changed since it started, the image selection would differ, SLURM jobs exist for later tasks, or any SSH/SLURM call fails.

## Apply and adoption

- `POST` locks the job row (`FOR UPDATE SKIP LOCKED`), writes a marker `RESUME boot=<server start> k=<k> <case> -> <action>` in `job.log` and commits once. The marker blocks a second resume of the job by the same server start (also from another worker) and lets an interrupted resume be recognised. Mark-only cases then zip the job folder, as at a normal job end.
- The relaunched job starts at task k with the replayed type filters. For each SLURM job of task k, the runner adopts the existing SLURM job (same stdout path) if its script is identical: if pending or running it keeps polling, if finished it fetches the outputs. Missing SLURM jobs are submitted; if the script of an existing one cannot be read, it is cancelled and resubmitted.
- SLURM jobs are listed with `GET /slurmdb/<v>/jobs` (finished jobs) and `GET /slurm/<v>/jobs` (active jobs).

## Task end in a single commit

At the end of each task, `execute_tasks` saves the executor error log first, then the dataset images, image-cache cleanup, accounting record and task-run status in **one commit** (for all runners). A crash in the middle leaves nothing saved, so an open task run always means "results not saved". This is what makes case C6 safe.

## Limitations

- **Hard crashes only.** On a graceful shutdown (e.g. `SIGTERM` during a deployment), running SLURM jobs are cancelled and the job is marked as failed, as without resume.
- **One server instance at a time.** The lock file is local to the machine/container, so two instances (e.g. two Kubernetes pods during a rolling update) could both resume the same jobs, and the old one cancels them at its shutdown. On Kubernetes, use the `Recreate` deployment strategy (`replicas: 1` alone is not enough).
- If a single gunicorn worker dies while the server keeps running, its jobs started after the server start, so they are not listed as orphans. They can be resumed with `POST ...?force=true`, but only once no live worker runs them.
- The shared start time relies on `--logger-class fractal_server.gunicorn_fractal.FractalGunicornLogger`; without it, each worker uses its own start time.
- There is no limit on the number of resumes of the same job.

## Testing

In `tests/v2/test_08_backends/`, no cluster needed: `test_resume_plan_unit.py`, `test_resume_startup_unit.py`, `test_slurm_rest_adopt_unit.py`.
