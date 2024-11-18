from pathlib import Path

from fractal_server.app.schemas.v2 import TaskCreateV2
from fractal_server.logger import get_logger
from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.utils import execute_command_sync


def _customize_and_run_template(
    template_filename: str,
    replacements: list[tuple[str, str]],
    script_dir: str,
    logger_name: str,
    prefix: int,
) -> str:
    """
    Customize one of the template bash scripts.

    Args:
        template_filename: Filename of the template file (ends with ".sh").
        replacements: Dictionary of replacements.
        script_dir: Local folder where the script will be placed.
        prefix: Prefix for the script filename.
    """
    logger = get_logger(logger_name=logger_name)
    logger.debug(f"_customize_and_run_template {template_filename} - START")

    # Prepare name and path of script
    if not template_filename.endswith(".sh"):
        raise ValueError(
            f"Invalid {template_filename=} (it must end with '.sh')."
        )

    script_filename = f"{prefix}{template_filename}"
    script_path_local = Path(script_dir) / script_filename
    # Read template
    customize_template(
        template_name=template_filename,
        replacements=replacements,
        script_path=script_path_local,
    )
    cmd = f"bash {script_path_local}"
    logger.debug(f"Now run '{cmd}' ")
    stdout = execute_command_sync(command=cmd, logger_name=logger_name)
    logger.debug(f"_customize_and_run_template {template_filename} - END")
    return stdout


def check_task_files_exist(task_list: list[TaskCreateV2]) -> None:
    """
    Check that the modules listed in task commands point to existing files.

    Args:
        task_list:
    """
    for _task in task_list:
        if _task.command_non_parallel is not None:
            _task_path = _task.command_non_parallel.split()[1]
            if not Path(_task_path).exists():
                raise FileNotFoundError(
                    f"Task `{_task.name}` has `command_non_parallel` "
                    f"pointing to missing file `{_task_path}`."
                )
        if _task.command_parallel is not None:
            _task_path = _task.command_parallel.split()[1]
            if not Path(_task_path).exists():
                raise FileNotFoundError(
                    f"Task `{_task.name}` has `command_parallel` "
                    f"pointing to missing file `{_task_path}`."
                )
