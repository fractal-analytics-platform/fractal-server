# SLURM REST runner

For `slurm_ssh` resources, the SLURM REST runner submits, polls and cancels SLURM jobs through [`slurmrestd`](https://slurm.schedmd.com/rest.html) (HTTPS), instead of running `sbatch`, `squeue` and `scancel` over SSH. All other parts of job execution are the same as for the SLURM/SSH runner: `SlurmRestRunner` is a subclass of `SlurmSSHRunner` that only overrides these three operations.

## Enabling it

The resource type stays `slurm_ssh`. When `FRACTAL_SLURM_REST_ENABLED=true`, **all** `slurm_ssh` resources use the REST runner (the setting is read for each job, and there is no fallback to SSH). The other settings are described in [`SlurmRestSettings`](../../configuration.md#fractal_server.runner.executors.slurm_rest.settings.SlurmRestSettings); in short:

| Variable | Default | Meaning |
|---|---|---|
| `FRACTAL_SLURM_REST_ENABLED` | `false` | `true` to use the REST runner |
| `FRACTAL_SLURM_REST_URL` | – | Base URL of `slurmrestd` (without `/slurm/<version>`); required if enabled |
| `FRACTAL_SLURM_REST_API_VERSION` | `v0.0.44` | OpenAPI plugin version |
| `FRACTAL_SLURM_REST_VERIFY_TLS` | `true` | Verify the TLS certificate of `slurmrestd` |
| `FRACTAL_SLURM_REST_CA_BUNDLE` | – | PEM file with the CAs to trust (e.g. an internal CA); if set, it replaces the system CAs |

These settings are global, so all `slurm_ssh` resources share the same `slurmrestd` instance.

## How it works

- **Authentication**: the server runs `scontrol token lifespan=3600` over SSH, as the profile user. The token is only kept in memory (never logged) and sent in the `X-SLURM-USER-NAME` / `X-SLURM-USER-TOKEN` headers. It is refreshed at 80% of its lifespan, or once after an HTTP 401/511 response.
- **Submission**: the `#SBATCH` lines of the submission script are converted into the JSON job description, since `slurmrestd` ignores them. An unsupported option is an error, also at runner start for the resource `extra_lines`. If `slurmrestd` warns about an unknown field, the job is cancelled and the submission fails.
- **No upload**: the job script writes its input/args JSON files (via quoted heredocs) and a copy of itself (`cp "$0"`) in the remote folder. Submission scripts are limited to 4 MB.
- **Environment**: the job gets the same `PATH` and `HOME` as with `sbatch` over SSH (read via `printenv` at runner start), and `SLURM_CPUS_PER_TASK` is set explicitly.
- **Polling**: one `GET /jobs` call per poll. Transient failures are tolerated up to 20 consecutive failed queries.
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
- `scontrol token` (authentication);
- `printenv HOME` and `printenv PATH`;
- remote `mkdir`;
- fetching outputs (file list, `tar` and SFTP).

## Known limitations

- Some `#SBATCH` to JSON mappings (`gres`, `gpus`, `exclude`, `nodelist`, `nodes`, `qos`) are not yet verified on a real cluster.
- A `slurmrestd` outage longer than the polling tolerance makes the job fail.

## Testing

- `tests/v2/test_08_backends/test_slurm_rest_unit.py`: unit tests, no cluster needed.
- `tests/v2/test_08_backends/test_slurm_rest_runner.py`: needs the SLURM docker containers (a fake REST client runs `sbatch` over SSH).
- `tests/v2/test_08_backends/test_slurm_rest_live.py`: opt-in tests against a real `slurmrestd`, skipped unless the `FRACTAL_TEST_SLURM_REST_*` environment variables are set (see the module docstring).
