from pathlib import Path
from typing import Optional

from fractal_server.logger import get_logger
from fractal_server.tasks.v2.utils_templates import customize_template
from fractal_server.utils import execute_command_sync


def _customize_and_run_template(
    template_filename: str,
    replacements: list[tuple[str, str]],
    script_dir: str,
    logger_name: str,
    prefix: Optional[int] = None,
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

    template_filename_stripped = template_filename[:-3]

    if prefix is not None:
        script_filename = f"{prefix}{template_filename_stripped}"
    else:
        script_filename = template_filename_stripped
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
    logger.debug(f"Standard output of '{cmd}':\n{stdout}")
    logger.debug(f"_customize_and_run_template {template_filename} - END")
    return stdout
