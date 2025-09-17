from fractal_server.config import PixiSLURMConfig
from fractal_server.ssh._fabric import FractalSSH


def run_script_on_remote_slurm(
    *,
    script_path: str,
    slurm_config: PixiSLURMConfig,
    fractal_ssh: FractalSSH,
    logger_name: str,
):
    pass
    sbatch_cmd = (
        "sbatch "
        f"-c {slurm_config.cpus} "
        f"-m {slurm_config.mem} "
        f"-t {slurm_config.time} "
        f"--wrap 'bash {script_path}'"
    )
    print(sbatch_cmd)
    stdout = fractal_ssh.run_command(cmd=sbatch_cmd)
    try:
        job_id = int(stdout)
    except Exception:
        # log and fail, or something like that
        raise 2
