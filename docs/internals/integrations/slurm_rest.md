# SLURM REST runner

**TODO**: this page is completed in PR 3 below (decision taken when splitting #3491: the full design document is added together with the runner).

The SLURM REST runner is added in four PRs, split from #3491:

1. REST client and `#SBATCH`-to-JSON translation (`fractal_server/runner/executors/slurm_rest/client.py`, `job_description.py`, `job_script.py`);
2. SSO token provider (`fractal_server/runner/executors/slurm_rest/token.py`);
3. `slurm_rest` resource type and `SlurmRestRunner`, with this page;
4. `GET /api/v2/job/squeue/` through `slurmrestd`.

Until PR 3, the full design is in the reference branch: [`slurm_rest.md` on `feat/slurm-rest-runner`](https://github.com/fractal-analytics-platform/fractal-server/blob/feat/slurm-rest-runner/docs/internals/integrations/slurm_rest.md).
