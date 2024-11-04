from pathlib import Path

TEMPLATES_DIR = Path(__file__).parent / "templates"


def customize_template(
    *,
    template_name: str,
    replacements: list[tuple[str, str]],
    script_path: str,
) -> str:
    """
    Customize a bash-script template and write it to disk.

    Args:
        template_filename:
        templates_folder:
        replacements:
    """
    # Read template
    template_path = TEMPLATES_DIR / template_name
    with template_path.open("r") as f:
        template_data = f.read()
    # Customize template
    script_data = template_data
    for old_new in replacements:
        script_data = script_data.replace(old_new[0], old_new[1])
    # Write script locally
    with open(script_path, "w") as f:
        f.write(script_data)


def parse_script_5_stdout(stdout: str) -> dict[str, str]:
    """
    Parse standard output of template 5.
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
