import shutil
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


def update_pyproject_toml(
    *,
    path: str,
    environment: str | None = None,
    target_platform: str | None = None,
):
    """
    https://github.com/fractal-analytics-platform/fractal-server/issues/2653#issuecomment-3035035436
    """
    if not path.endswith(".toml"):
        raise ValueError()

    shutil.copy(src=path, dst=f"{path}.backup")

    with open(path, "rb") as fp:
        data = tomllib.load(fp)

    if target_platform is not None:
        data["tool"]["pixi"]["workspace"]["platforms"] = [target_platform]

    if environment is not None:
        environments = data["tool"]["pixi"]["environments"]
        data["tool"]["pixi"]["environments"] = {
            key: value
            for key, value in environments.items()
            if key == environment
        }

    toml_string = tomli_w.dumps(data)

    # Additional validation
    tomllib.loads(toml_string)

    with open(path, "w") as fp:
        fp.write(toml_string)
