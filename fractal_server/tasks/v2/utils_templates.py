from pathlib import Path

from fractal_server.app.models.v2 import TaskGroupV2
from fractal_server.config import get_settings
from fractal_server.logger import set_logger
from fractal_server.syringe import Inject

TEMPLATES_DIR = Path(__file__).parent / "templates"

SCRIPTS_SUBFOLDER = "scripts"

logger = set_logger(__name__)


def customize_template(
    *,
    template_name: str,
    replacements: list[tuple[str, str]],
    script_path: str,
) -> str:
    """
    Customize a bash-script template and write it to disk.

    Args:
        template_name: Name of the template that will be customized.
        replacements: List of replacements for template customization.
        script_path: Local path where the customized template will be written.
    """
    # Read template
    template_path = TEMPLATES_DIR / template_name
    with template_path.open("r") as f:
        template_data = f.read()
    # Customize template
    script_data = template_data
    for old_new in replacements:
        script_data = script_data.replace(old_new[0], old_new[1])
    # Create parent folder if needed
    Path(script_path).parent.mkdir(exist_ok=True)
    # Write script locally
    with open(script_path, "w") as f:
        f.write(script_data)


def parse_script_pip_show_stdout(stdout: str) -> dict[str, str]:
    """
    Parse standard output of 4_pip_show.sh
    """
    searches = [
        ("Python interpreter:", "python_bin"),
        ("Package name:", "package_name"),
        ("Package version:", "package_version"),
        ("Package parent folder:", "package_root_parent"),
        ("Manifest absolute path:", "manifest_path"),
    ]
    stdout_lines = stdout.splitlines()
    attributes = dict()
    for search, attribute_name in searches:
        matching_lines = [_line for _line in stdout_lines if search in _line]
        if len(matching_lines) == 0:
            raise ValueError(f"String '{search}' not found in stdout.")
        elif len(matching_lines) > 1:
            raise ValueError(
                f"String '{search}' found too many times "
                f"({len(matching_lines)})."
            )
        else:
            actual_line = matching_lines[0]
            attribute_value = actual_line.split(search)[-1].strip(" ")
            attributes[attribute_name] = attribute_value
    return attributes


def get_collection_replacements(
    *, task_group: TaskGroupV2, python_bin: str
) -> dict[str, str]:
    settings = Inject(get_settings)

    replacements = [
        ("__PACKAGE_NAME__", task_group.pkg_name),
        ("__PACKAGE_ENV_DIR__", task_group.venv_path),
        ("__PYTHON__", python_bin),
        ("__INSTALL_STRING__", task_group.pip_install_string),
        (
            "__FRACTAL_MAX_PIP_VERSION__",
            settings.FRACTAL_MAX_PIP_VERSION,
        ),
        ("__FRACTAL_PIP_CACHE_DIR_ARG__", settings.PIP_CACHE_DIR_ARG),
        (
            "__PINNED_PACKAGE_LIST__",
            task_group.pinned_package_versions_string,
        ),
    ]
    logger.info(
        f"Cache-dir argument for `pip install`: {settings.PIP_CACHE_DIR_ARG}"
    )
    return replacements
