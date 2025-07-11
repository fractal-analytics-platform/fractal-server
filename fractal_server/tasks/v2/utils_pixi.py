from typing import TypedDict

import tomli_w
import tomllib

SOURCE_DIR_NAME = "source_dir"


class ParsedOutput(TypedDict):
    package_root: str
    venv_size: str
    venv_file_number: str
    project_python_wrapper: str


def parse_collect_stdout(stdout: str) -> ParsedOutput:
    """
    Parse standard output of `pixi/1_collect.sh`
    """
    searches = [
        ("Package folder:", "package_root"),
        ("Disk usage:", "venv_size"),
        ("Number of files:", "venv_file_number"),
        ("Project Python wrapper:", "project_python_wrapper"),
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


def simplify_pyproject_toml(
    *,
    original_toml_string: str,
    pixi_environment: str,
    pixi_platform: str,
) -> str:
    """
    Simplify `pyproject.toml` contents to make `pixi install` less heavy.

    Args:
        original_toml_string: Original `pyproject.toml` contents
        environment: Name of the pixi environment to use (e.g. `default`).
        target_platform: Name of the platform (e.g. `linux-64`)

    Returns:
        New `pyproject.toml` contents
    """

    # Parse TOML string
    data = tomllib.loads(original_toml_string)

    # Use a single platform
    data["tool"]["pixi"]["workspace"]["platforms"] = [pixi_platform]

    # Keep a single environment
    current_environments = data["tool"]["pixi"]["environments"]
    data["tool"]["pixi"]["environments"] = {
        key: value
        for key, value in current_environments.items()
        if key == pixi_environment
    }

    # Drop pixi.tasks
    data["tool"]["pixi"].pop("tasks", None)

    # Prepare and validate new `pyprojectl.toml` contents
    new_toml_string = tomli_w.dumps(data)
    tomllib.loads(new_toml_string)

    return new_toml_string
