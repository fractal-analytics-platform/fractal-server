import os
from pathlib import Path

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.logger import get_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.tasks.v2.utils_templates import customize_template


def _customize_and_run_template(
    *,
    template_filename: str,
    replacements: list[tuple[str, str]],
    script_dir_local: str,
    prefix: str,
    fractal_ssh: FractalSSH,
    script_dir_remote: str,
    logger_name: str,
) -> str:
    """
    Customize one of the template bash scripts, transfer it to the remote host
    via SFTP and then run it via SSH.

    Args:
        template_filename: Filename of the template file (ends with ".sh").
        replacements: Dictionary of replacements.
        script_dir: Local folder where the script will be placed.
        prefix: Prefix for the script filename.
        fractal_ssh: FractalSSH object
        script_dir_remote: Remote scripts directory
    """
    logger = get_logger(logger_name=logger_name)
    logger.debug(f"_customize_and_run_template {template_filename} - START")
    # Prepare name and path of script
    if not template_filename.endswith(".sh"):
        raise ValueError(
            f"Invalid {template_filename=} (it must end with '.sh')."
        )
    script_filename = f"{prefix}_{template_filename}"
    script_path_local = (Path(script_dir_local) / script_filename).as_posix()

    customize_template(
        template_name=template_filename,
        replacements=replacements,
        script_path=script_path_local,
    )

    # Transfer script to remote host
    script_path_remote = os.path.join(
        script_dir_remote,
        script_filename,
    )
    logger.debug(f"Now transfer {script_path_local=} over SSH.")
    fractal_ssh.send_file(
        local=script_path_local,
        remote=script_path_remote,
    )

    # Execute script remotely
    cmd = f"bash {script_path_remote}"
    logger.debug(f"Now run '{cmd}' over SSH.")
    stdout = fractal_ssh.run_command(cmd=cmd)

    logger.debug(f"_customize_and_run_template {template_filename} - END")
    return stdout


def _copy_wheel_file_ssh(
    *, task_group: TaskGroupV2, fractal_ssh: FractalSSH, logger_name: str
) -> str:
    """
    Handle the situation where `task_group.wheel_path` is not part of
    `task_group.path`, by copying `wheel_path` into `path`.

    Returns:
        The new `wheel_path`.
    """
    logger = get_logger(logger_name=logger_name)
    source = task_group.wheel_path
    dest = (
        Path(task_group.path) / Path(task_group.wheel_path).name
    ).as_posix()
    cmd = f"cp {source} {dest}"
    logger.debug(f"[_copy_wheel_file] START {source=} {dest=}")
    fractal_ssh.run_command(cmd=cmd)
    logger.debug(f"[_copy_wheel_file] END {source=} {dest=}")
    return dest
