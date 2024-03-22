from pathlib import Path

from pydantic.decorator import validate_arguments


@validate_arguments
def apply_registration_to_image(
    *,
    path: str,
    overwrite_input: bool = True,
) -> dict:

    prefix = "[apply_registration_to_image]"
    print(f"{prefix} START")
    print(f"{prefix} {path=}")
    print(f"{prefix} {overwrite_input=}")

    table_path = Path(path) / "registration_table_final"
    print(f" Reading information from {table_path.as_posix()}")
    with table_path.open("r") as f:
        f.read()

    # Handle the case of path=ref_path
    if overwrite_input:
        out = dict(image_list_updates=[dict(path=path)])
        with (Path(path) / "data").open("a") as f:
            f.write("Applying registration\n")
    else:
        new_path = f"{path}_r"
        print(f"{prefix} {new_path=}")
        out = dict(image_list_updates=[dict(path=new_path, origin=path)])
        Path(new_path).mkdir()
        with (Path(new_path) / "data").open("a") as f:
            f.write("Applying registration\n")
    print(f"{prefix} {out=}")
    print(f"{prefix} END")
    return out
