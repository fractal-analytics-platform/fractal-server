# SLURM REST runner

**NOTE** - Temporary doc to summarize the changes in sight of the PR

For `slurm_rest` resources, the SLURM REST runner submits, polls and cancels SLURM jobs through [`slurmrestd`](https://slurm.schedmd.com/rest.html) (HTTPS), instead of running `sbatch`, `squeue` and `scancel` over SSH. `SlurmRestRunner` is a subclass of `SlurmSSHRunner`: besides these three operations, it changes how the job inputs reach the cluster (embedded in the submission script, no upload), the mode of the remote folders, the job environment, and the wait after a shutdown request (see **How it works**). The rest (task execution, fetching outputs, ...) is the same as for the SLURM/SSH runner.

## Enabling it

Set `FRACTAL_RUNNER_BACKEND=slurm_rest`, and create `slurm_rest` resources and profiles. They have the same fields as `slurm_ssh` ones (SSH is still used, see **SSH is still required**, and task collection works as for `slurm_ssh` resources), and the resource `jobs_runner_config` has a required `rest` key (see [`JobRunnerConfigSlurmRest`](./runners.md#fractal_server.runner.config._slurm_rest.JobRunnerConfigSlurmRest)):

| `jobs_runner_config.rest` key | Required | Meaning |
|---|---|---|
| `url` | yes | HTTPS base URL of `slurmrestd` (without `/slurm/<version>`) |
| `api_version` | yes | `slurmrestd` OpenAPI version, one of `v0.0.40` to `v0.0.45` (`SUPPORTED_API_VERSIONS`), and served by `slurmrestd` |
| `ca_bundle` | no | PEM file (on the `fractal-server` machine) with the CAs to trust (e.g. an internal CA); if set, it replaces the system CAs. TLS verification cannot be disabled |

For example:
```json
"rest": {"url": "https://slurmrestd.example.org", "api_version": "v0.0.44", "ca_bundle": "/etc/ssl/internal-ca.pem"}
```

**API versions**: the requests and the response fields used by the runner are the same in all supported versions, `v0.0.40` (Slurm 23.11) to `v0.0.45` (Slurm 26.05). This was checked on the OpenAPI specs of `v0.0.40` to `v0.0.44` and the OpenAPI release notes for `v0.0.45`, and tested on a cluster with `v0.0.40` to `v0.0.44`. A `slurmrestd` serves several versions (e.g. Slurm 25.11 serves `v0.0.41` to `v0.0.44`). The version is not discovered at each job: if the configured one is not served (`GET /slurm/<v>/ping` answers 404), the job fails and the error lists the supported versions that `slurmrestd` serves. Supporting a new version means adding it to `SUPPORTED_API_VERSIONS`, after checking the paths and fields used (`client.py`, `job_description.py`).

Each `slurm_rest` resource describes one cluster, so several clusters (each with its own `slurmrestd`) need several resources.

`FRACTAL_SLURM_REST_TOKEN` (see [`Settings`](../../configuration.md#fractal_server.config._main.Settings)) is a **development-only** setting: a fixed token used for all requests of all users, instead of their SSO token (never refreshed).

## How it works

- **Authentication**: Token sources:
    - SSO: the OAuth/OIDC access token that fractal-server stores (table `oauthaccount`) at every SSO login of the user. It is used if it expires in more than 1 minute; otherwise it is refreshed with the stored refresh token and the configured OAuth client (`OAUTH_*` settings), and the new tokens are saved. The refresh locks the database row, so that concurrent runners (threads or server workers) refresh it only once. The user must have exactly one SSO account; its `account_id` (the `sub` claim) is the SLURM user, which may differ from `Profile.username` (e.g. a shared SSH service account). `slurmrestd` must trust the identity provider. If the token cannot be refreshed (no refresh token, or refresh refused), the user must log in to Fractal again: at runner start the job fails with this hint; while polling, the job waits (see **Polling**) until a new login provides a valid token. A refresh token refused by the identity provider (HTTP 400 `invalid_grant`) is not sent again; other errors (network, server, client configuration) are retried at the next poll.
    - `FRACTAL_SLURM_REST_TOKEN` (development only, e.g. a token from `scontrol token` for local tests): if set, this token is used instead, for all users, and a warning is logged at each runner start. The SLURM user is its `sub` or `sun` claim (else `Profile.username`), and a rejection is an error.
- **SSH user and SLURM user**: SSH always runs as `Profile.username`; SLURM jobs run as the token user, i.e. the SSO `sub`. Both are logged at runner start
- **Submission**: the `#SBATCH` lines of the submission script are converted into the JSON job description, since `slurmrestd` ignores them. An unsupported option is an error. Values are read as `sbatch` reads them, except memory units, which follow Fractal's configuration. If `slurmrestd` warns about an unknown field, the job is cancelled and the submission fails. The mappings were checked against `sbatch` on cluster (Slurm 25.11).
- **No upload**: the job script writes its input/args JSON files (via quoted heredocs) and a copy of itself (`cp "$0"`) in the remote folder, with `umask 0002`. Remote folders are created (over SSH) with mode `2775`. Submission scripts are limited to 4 MB.
- **Environment**: the job gets the same `PATH` as with `sbatch` over SSH (`printenv PATH` at runner start), `HOME` and `USER` of the SLURM user (`getent passwd`), and `SLURM_CPUS_PER_TASK` is set explicitly. Unlike with `sbatch`, the job does not get the environment of a login shell (e.g. no `module` command, no variables exported in `~/.bashrc`, ...). Configuration files are read from the SLURM user's `HOME` by default (e.g. `~/.aws/config`), so they may differ between users. For the same configuration for all user it is possible to the resource `extra_lines`, which are copied as is into every submission script, or let tasks configure themselves.
- **Polling**: one `GET /jobs` call per poll. Job states are read as `squeue` reports them: `slurmrestd` returns the base state and the flags of each job, so that SLURM jobs are considered finished at the same time as with the SLURM/SSH runner. If the job states cannot be queried (no valid token, or `slurmrestd` unreachable, answering with errors, ...), the SLURM jobs are considered still running, and the query is repeated at the next poll, with no limit: a failed query never marks SLURM jobs as completed, and never fails the Fractal job. The job stays `submitted` and resumes as soon as a query succeeds (e.g. after a new SSO login, or when `slurmrestd` is back); SLURM jobs that finished in the meantime are then processed normally. A warning is logged once when the problem starts, and an info message when it ends.
- **`GET /api/v2/job/squeue/?scope=user`**: for `slurm_rest` resources, still runs `squeue` over SSH, but filters on the SLURM user (the SSO `sub`) instead of `Profile.username`, so that users see their own REST jobs.
- **Restart behaviour**: the same as for the SLURM/SSH runner. On graceful shutdown, running SLURM jobs are cancelled (through `slurmrestd`) and the job is marked as failed.

| REST call | When |
|---|---|
| `GET /slurm/<v>/ping` | Runner start, before any SSH command (if it fails, the job fails with a `Cannot reach slurmrestd` error, with the "log in again" hint if the token cannot be refreshed, or with the served API versions if `<v>` is not served; nothing is submitted and no remote folder is created) |
| `POST /slurm/<v>/job/submit` | Each SLURM job submission |
| `GET /slurm/<v>/jobs` | Each poll |
| `DELETE /slurm/<v>/job/{id}` | Job stop/shutdown, or unknown-field warning |

## SSH is still required

The REST runner still uses SSH (with the same resource/profile configuration as the SLURM/SSH runner) for:

- connection and worker-version checks;
- `printenv PATH` and `getent passwd <slurm user>`;
- remote `mkdir`;
- fetching outputs (file list, `tar` and SFTP).

## Cluster requirements when SSH and SLURM users differ

E.g. with a shared SSH service account in the profiles:

- the `sub` of each Fractal user is a UNIX user on the cluster (checked with `getent passwd` at runner start: otherwise the job fails with a `SLURM user ... is unknown on the cluster` error);
- the SSH user and the SLURM users share a UNIX group, which owns the jobs folder;
- task environments and the worker Python are readable/executable by the SLURM users, and the image data is accessible to them;
- the SLURM users are associated with the SLURM account(s) set in Fractal.

## Known limitations

- If job states cannot be queried (no valid SSO token, or `slurmrestd` down or failing), a running job waits with no time limit (see **Polling**):
    - the user is not notified (**TODO**): the Fractal job just stays `submitted`, and only a server log warning is written;
    - stopping the Fractal job while waiting does not end it: the cancel requests fail as well, and cancelling and polling are retried at each poll (every `jobs_poll_interval`), so that its SLURM jobs stay tracked. The Fractal job stays `submitted` until a request succeeds, and then fails as any stopped job. If the problem never ends (e.g. the user never logs in again), it stays `submitted` until the server shuts down;
    - when the server shuts down gracefully (e.g. restart), the Fractal job is marked as failed after `FRACTAL_GRACEFUL_SHUTDOWN_TIME` (as any job still running), but its SLURM jobs cannot be cancelled: they keep running, untracked, until they end.
- Submission is not retried: if the token is missing or `slurmrestd` fails when a task submits its SLURM jobs, that task fails (as when `sbatch` fails with the SLURM/SSH runner).

## Testing

- `tests/v2/test_08_backends/test_slurm_rest_unit.py`: unit tests
- `tests/v2/test_08_backends/test_slurm_rest_token.py`: token providers (SSO refresh with a fake identity provider), needs the test database.
- `tests/v2/test_08_backends/test_slurm_rest_runner.py`: needs the SLURM docker containers, which have no `slurmrestd`. The real `SlurmRestRunner` runs with a fake REST client (`tests/v2/_aux_slurm_rest.py`) that submits the generated script with `sbatch` over SSH (and polls/cancels with `squeue`/`scancel`). This checks what the runner builds (script with embedded inputs, JSON job description, SLURM user in the requests, `USER`/`HOME`) and the submit/poll/fetch cycle on a real cluster.
- `tests/v2/test_07_full_workflow/test_full_workflow_slurm_rest.py`: a full job, from the API to `SlurmRestRunner`, with the same fake REST client; needs the SLURM docker containers.
- `tests/v2/test_08_backends/test_slurm_rest_live.py`: opt-in tests against a real `slurmrestd` (JSON job description, job states, cancellation), skipped unless the `FRACTAL_TEST_SLURM_REST_*` environment variables are set (see the module docstring). To discuss if we ever want this.

Inherited behaviour (task failures, compound tasks, fetching outputs, ...) is covered by the SLURM/SSH and SLURM/sudo runner tests. What `SlurmRestRunner` changes, including polling and cancellation during a shutdown request while `slurmrestd` cannot be reached, is covered by the tests above.
