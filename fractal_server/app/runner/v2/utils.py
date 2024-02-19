import json


def pjson(x: dict) -> str:
    """
    Naive JSON pretty-print.
    """
    return json.dumps(x, indent=2)


def indent(s: str, num_spaces: int = 2):
    indentation = " " * num_spaces
    new_lines = [f"{indentation}{line.rstrip()}" for line in s.split("\n")]
    return "\n".join(new_lines)


def ipjson(x: dict) -> str:
    return indent(pjson(x), num_spaces=2)
