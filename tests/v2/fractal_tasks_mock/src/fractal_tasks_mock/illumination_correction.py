from pathlib import Path

from pydantic.decorator import validate_arguments


@validate_arguments
def illumination_correction(
    *,
    path: str,
    overwrite_input: bool = False,
) -> dict:

    print("[illumination_correction] START")
    print(f"[illumination_correction] {path=}")
    print(f"[illumination_correction] {overwrite_input=}")

    # Prepare output metadata and set actual_path
    if overwrite_input:
        out = dict(image_list_updates=[dict(path=path)])
        actual_path = path
    else:
        new_path = f"{path}_corr"
        Path(new_path).mkdir(exist_ok=True)
        out = dict(image_list_updates=[dict(path=new_path, origin=path)])
        actual_path = new_path
        print(f"[illumination_correction] {new_path=}")

    with (Path(actual_path) / "data").open("a") as f:
        f.write(f"[illumination_correction] ({overwrite_input=})\n")

    print("[illumination_correction] END")
    return out
