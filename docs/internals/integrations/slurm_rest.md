# SLURM REST runner

For `slurm_ssh` resources, the SLURM REST runner submits, polls and cancels SLURM jobs through [`slurmrestd`](https://slurm.schedmd.com/rest.html) (HTTPS), instead of running `sbatch`, `squeue` and `scancel` over SSH. All other parts of job execution are the same as for the SLURM/SSH runner: `SlurmRestRunner` is a subclass of `SlurmSSHRunner` that only overrides these three operations.

## Enabling it

The resource type stays `slurm_ssh`. When `FRACTAL_SLURM_REST_ENABLED=true`, **all** `slurm_ssh` resources use the REST runner (settings are read at server startup, and invalid values make it fail; there is no fallback to SSH). The other settings are described in [`SlurmRestSettings`](../../configuration.md#fractal_server.runner.executors.slurm_rest.settings.SlurmRestSettings); in short:

| Variable | Default | Meaning |
|---|---|---|
| `FRACTAL_SLURM_REST_ENABLED` | `false` | `true` to use the REST runner |
| `FRACTAL_SLURM_REST_URL` | – | HTTPS base URL of `slurmrestd` (without `/slurm/<version>`); required if enabled |
| `FRACTAL_SLURM_REST_CA_BUNDLE` | – | PEM file with the CAs to trust (e.g. an internal CA); if set, it replaces the system CAs. TLS verification cannot be disabled |
| `FRACTAL_SLURM_REST_TOKEN` | – | **Development only**: fixed token used for all requests of all users, instead of their SSO token (never refreshed) |

These settings are global, so all `slurm_ssh` resources share the same `slurmrestd` instance. The `slurmrestd` OpenAPI version is fixed in the code (`v0.0.44`, `SLURM_REST_API_VERSION`), since the JSON job description and the response parsing follow it. The settings describe a cluster, and should move to `Resource` (e.g. a dedicated `slurm_rest` resource type), as `host` and the other cluster settings.

## How it works

- **Authentication**: tokens are sent in the `X-SLURM-USER-NAME` / `X-SLURM-USER-TOKEN` headers and never logged. After an HTTP 401/511 response, the request is retried once with a new token. Token sources:
    - SSO: the OAuth/OIDC access token that fractal-server stores (table `oauthaccount`) at every SSO login of the user. It is used if it expires in more than 1 minute; otherwise (or after a 401/511) it is refreshed with the stored refresh token and the configured OAuth client (`OAUTH_*` settings), and the new tokens are saved. The refresh locks the database row, so that concurrent runners (threads or server workers) refresh it only once. The user must have exactly one SSO account; its `account_id` (the `sub` claim) is the SLURM user, which may differ from `Profile.username` (e.g. a shared SSH service account). `slurmrestd` must trust the identity provider (`AuthAltTypes=auth/jwt`, its keys in `jwks`, `userclaimfield=sub`). If the token cannot be refreshed (no refresh token, or refresh refused), the user must log in to Fractal again: at runner start the job fails with this hint; while polling, the job waits (see **Polling**) until a new login provides a valid token. A refresh token refused by the identity provider (HTTP 400 `invalid_grant`) is not sent again; other errors (network, server, client configuration) are retried at the next poll.
    - `FRACTAL_SLURM_REST_TOKEN` (development only, e.g. a token from `scontrol token` for local tests): if set, this token is used instead, for all users, and a warning is logged at each runner start. The SLURM user is its `sub` or `sun` claim (else `Profile.username`), and a rejection is an error.
- **SSH user and SLURM user**: SSH always runs as `Profile.username`; SLURM jobs run as the token user (`X-SLURM-USER-NAME`, `USER` and `HOME` of the job), i.e. the SSO `sub`. Both are logged at runner start, e.g. `SLURM REST runner ready (..., ssh user=svc, slurm user=alice)`; tokens are never logged.
- **Submission**: the `#SBATCH` lines of the submission script are converted into the JSON job description, since `slurmrestd` ignores them. An unsupported option is an error, also at runner start for the resource `extra_lines`. If `slurmrestd` warns about an unknown field, the job is cancelled and the submission fails. The mappings were checked against `sbatch` on JUNO (Slurm 25.11): the same options give the same `scontrol show job` values. As with `sbatch`, `--nodes` is lowered to `--ntasks` when larger.
- **No upload**: the job script writes its input/args JSON files (via quoted heredocs) and a copy of itself (`cp "$0"`) in the remote folder, with `umask 0002`. Remote folders are created (over SSH) with mode `2775`. Submission scripts are limited to 4 MB.
- **Environment**: the job gets the same `PATH` as with `sbatch` over SSH (`printenv PATH` at runner start), `HOME` and `USER` of the SLURM user (`getent passwd`), and `SLURM_CPUS_PER_TASK` is set explicitly.
- **Polling**: one `GET /jobs` call per poll. If the job states cannot be queried (no valid token, or `slurmrestd` unreachable or answering with errors), the SLURM jobs are considered still running, and the query is repeated at the next poll, with no limit: a failed query never marks SLURM jobs as completed, and never fails the Fractal job. The job stays `submitted` and resumes as soon as a query succeeds (e.g. after a new SSO login, or when `slurmrestd` is back); SLURM jobs that finished in the meantime are then processed normally. A warning is logged once when the problem starts (`[run_squeue] No valid SLURM token, waiting for a new login` or `[run_squeue] Cannot query SLURM job states, retrying`), and an info message when it ends (`[run_squeue] SLURM job states available again.`).
- **Restart behaviour**: the same as for the SLURM/SSH runner. On graceful shutdown, running SLURM jobs are cancelled (through `slurmrestd`) and the job is marked as failed.

| REST call | When |
|---|---|
| `GET /slurm/<v>/ping` | Runner start |
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
- the SSH user and the SLURM users share a UNIX group, which owns the jobs folder (`Profile.jobs_remote_dir`, with setgid);
- task environments (`Profile.tasks_remote_dir`) and the worker Python are readable/executable by the SLURM users, and the image data is accessible to them;
- the SLURM users are associated with the SLURM account(s) set in Fractal.

## Known limitations

- If job states cannot be queried (no valid SSO token, or `slurmrestd` down or failing), a running job waits with no time limit (see **Polling**):
    - the user is not notified: the Fractal job just stays `submitted`, and only a server log warning is written;
    - stopping the job (or a graceful server shutdown) while waiting marks it as failed, but cannot cancel its SLURM jobs (the cancel request fails as well), which run until they end;
    - if the problem never ends (e.g. the user never logs in again), the job stays `submitted` until it is stopped.
- Submission is not retried: if the token is missing or `slurmrestd` fails when a task submits its SLURM jobs, that task fails (as when `sbatch` fails with the SLURM/SSH runner).

## Testing

- `tests/v2/test_08_backends/test_slurm_rest_unit.py`: unit tests, no cluster needed.
- `tests/v2/test_08_backends/test_slurm_rest_token.py`: token providers (SSO refresh with a fake identity provider), needs the test database.
- `tests/v2/test_08_backends/test_slurm_rest_runner.py`: needs the SLURM docker containers (a fake REST client runs `sbatch` over SSH).
- `tests/v2/test_08_backends/test_slurm_rest_live.py`: opt-in tests against a real `slurmrestd`, skipped unless the `FRACTAL_TEST_SLURM_REST_*` environment variables are set (see the module docstring).
