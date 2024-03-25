from pathlib import Path

from pydantic.decorator import validate_arguments

from .input_models import InitArgsIllumination


@validate_arguments
def illumination_correction_compute(
    *,
    path: str,
    init_args: InitArgsIllumination,
) -> dict:
    """
    Dummy task description.
    """

    raw_path = init_args.raw_path
    subsets = init_args.subsets
    print("[illumination_correction_compute] START")
    print(f"[illumination_correction_compute] {path=}")
    print(f"[illumination_correction_compute] {raw_path=}")
    print(f"[illumination_correction_compute] {subsets=}")

    # Prepare output metadata and set actual_path
    if path == raw_path:
        out = dict(image_list_updates=[dict(path=path)])
    else:
        out = dict(image_list_updates=[dict(path=path, origin=raw_path)])
        print(f"[illumination_correction_compute] {path=}")

    with (Path(path) / "data").open("a") as f:
        f.write(
            f"[illumination_correction_compute] Running with {raw_path=}, "
            f"{subsets=}\n"
        )

    print("[illumination_correction_compute] END")
    return out
