"""
SLURM REST runner, for `slurm_rest` resources.

SLURM jobs are submitted, polled and cancelled through `slurmrestd`, as the
SLURM user of the Fractal user's SSO token (`sub` claim). SSH still runs as
`Profile.username` (e.g. a shared service account) for version checks,
`getent passwd`/`printenv PATH`, folder creation and fetching outputs.
"""
