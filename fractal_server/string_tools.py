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
