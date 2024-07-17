import string

__SPECIAL_CHARACTERS__ = f"{string.punctuation}{string.whitespace}"


def sanitize_string(value: str) -> str:
    """
    Make string safe to be used in file/folder names and subprocess commands.

    Make the string lower-case, and replace any special character with an
    underscore, where special characters are:


        >>> string.punctuation
        '!"#$%&\'()*+,-./:;<=>?@[\\\\]^_`{|}~'
        >>> string.whitespace
        ' \\t\\n\\r\\x0b\\x0c'

    Args:
        value: Input string

    Returns:
        Sanitized value
    """
    new_value = value.lower()
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

    Args:
        task_name:

    Return:
        Slug-ified task name.
    """
    return task_name.replace(" ", "_").lower()
