import string


__SPECIAL_CHARACTERS__ = f"{string.punctuation}{string.whitespace}"

# List of invalid characters discussed here:
# https://github.com/fractal-analytics-platform/fractal-server/issues/1647
__NOT_ALLOWED_FOR_COMMANDS__ = r"`#$&*()\|[]{};<>?!"


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


def validate_cmd(
    command: str,
    *,
    allow_char: str | None = None,
    attribute_name: str = "Command",
):
    """
    Assert that the provided `command` does not contain any of the forbidden
    characters for commands
    (fractal_server.string_tools.__NOT_ALLOWED_FOR_COMMANDS__)

    Args:
        command: command to validate.
        allow_char: chars to accept among the forbidden ones
        attribute_name: Name of the attribute, to be used in error message.
    """
    if not isinstance(command, str):
        raise ValueError(f"{command=} is not a string.")
    forbidden = set(__NOT_ALLOWED_FOR_COMMANDS__)
    if allow_char is not None:
        forbidden = forbidden - set(allow_char)
    if not forbidden.isdisjoint(set(command)):
        raise ValueError(
            f"{attribute_name} must not contain any of this characters: "
            f"'{forbidden}'\n"
            f"Provided {attribute_name.lower()}: '{command}'."
        )
