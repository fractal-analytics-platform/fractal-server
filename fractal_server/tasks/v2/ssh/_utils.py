import os
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncSession

from ..utils_background import fail_and_cleanup
from ..utils_pixi import simplify_pyproject_toml
from fractal_server.app.models.v2 import TaskGroupActivityV2
from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.config import get_settings
from fractal_server.logger import get_logger
from fractal_server.logger import set_logger
from fractal_server.ssh._fabric import FractalSSH
from fractal_server.syringe import Inject
from fractal_server.tasks.v2.utils_templates import customize_template

logger = set_logger(__name__)


def _customize_and_run_template(
    *,
    template_filename: str,
    replacements: set[tuple[str, str]],
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
        script_dir_local: Local folder where the script will be placed.
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
    *,
    task_group: TaskGroupV2,
    fractal_ssh: FractalSSH,
    logger_name: str,
) -> str:
    """
    Handle the situation where `task_group.archive_path` is not part of
    `task_group.path`, by copying `archive_path` into `path`.

    Returns:
        The new `archive_path`.
    """
    logger = get_logger(logger_name=logger_name)
    source = task_group.archive_path
    dest = (
        Path(task_group.path) / Path(task_group.archive_path).name
    ).as_posix()
    cmd = f"cp {source} {dest}"
    logger.debug(f"[_copy_wheel_file_ssh] START {source=} {dest=}")
    fractal_ssh.run_command(cmd=cmd)
    logger.debug(f"[_copy_wheel_file_ssh] END {source=} {dest=}")
    return dest


def check_ssh_or_fail_and_cleanup(
    *,
    fractal_ssh: FractalSSH,
    task_group: TaskGroupV2,
    task_group_activity: TaskGroupActivityV2,
    logger_name: str,
    log_file_path: Path,
    db: AsyncSession,
) -> bool:
    """
    Check SSH connection.

    Returns:
        Whether SSH connection is OK.
    """
    try:
        fractal_ssh.check_connection()
        return True
    except Exception as e:
        logger = get_logger(logger_name=logger_name)
        logger.error(
            "Cannot establish SSH connection. " f"Original error: {str(e)}"
        )
        fail_and_cleanup(
            task_group=task_group,
            task_group_activity=task_group_activity,
            logger_name=logger_name,
            log_file_path=log_file_path,
            exception=e,
            db=db,
        )
        return False


def edit_pyproject_toml_in_place_ssh(
    *,
    fractal_ssh: FractalSSH,
    pyproject_toml_path: Path,
) -> None:
    """
    Wrapper of `simplify_pyproject_toml`, with I/O.
    """

    # Read `pyproject.toml`
    pyproject_contents = fractal_ssh.read_remote_text_file(
        pyproject_toml_path.as_posix()
    )

    # Simplify contents
    settings = Inject(get_settings)
    new_pyproject_contents = simplify_pyproject_toml(
        original_toml_string=pyproject_contents,
        pixi_environment=settings.pixi.DEFAULT_ENVIRONMENT,
        pixi_platform=settings.pixi.DEFAULT_PLATFORM,
    )
    # Write new `pyproject.toml`
    fractal_ssh.write_remote_file(
        path=pyproject_toml_path.as_posix(),
        content=new_pyproject_contents,
    )
    logger.debug(
        f"Replaced remote {pyproject_toml_path.as_posix()} "
        "with simplified version."
    )
