import string

__SPECIAL_CHARACTERS__ = f"{string.punctuation}{string.whitespace}"


def sanitize_string(value: str) -> str:
    """
    Make string safe to be used in file/folder names and subprocess commands.

    Replace any special character by an underscore, where special characters
    include:
    ```python repl
    >>> string.punctuation
    '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
    >>> string.whitespace
    ' \t\n\r\x0b\x0c'
    ```

    Args:
        value: Input string

    Returns:
        Sanitized value
    """
    new_value = value
    for character in __SPECIAL_CHARACTERS__:
        new_value = new_value.replace(character, "_")
    return new_value


def slugify_task_name_for_source(task_name: str) -> str:
    """
    NOTE: this function is used upon creation of tasks' sources, therefore
    for the moment we cannot replace it with its more comprehensive version
    from `fractal_server.string_tools.sanitize_string`, nor we can remove it.
    As 2.3.1, we are renaming it to `slugify_task_name_for_source`, to make
    it clear that it should not be used for other purposes.
    """
    return task_name.replace(" ", "_").lower()
