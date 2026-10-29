"""
Runner that submits, polls and cancels SLURM jobs through `slurmrestd`, with
the SSO token of the Fractal user, while still using SSH for version checks,
folder creation and artifact fetching.
"""
