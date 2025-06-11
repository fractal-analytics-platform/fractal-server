from typing import TypedDict

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
