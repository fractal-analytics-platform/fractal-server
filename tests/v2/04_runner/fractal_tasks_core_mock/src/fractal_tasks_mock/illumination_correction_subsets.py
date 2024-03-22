from pathlib import Path
from typing import Literal

from pydantic import BaseModel
from pydantic import Field
from pydantic.decorator import validate_arguments


class InitArgsIllumination(BaseModel):
    raw_path: str
    subsets: dict[Literal["T_index", "C_index", "Z_index"], int] = Field(
        default_factory=dict
    )


@validate_arguments
def illumination_correction_subsets(
    *,
    path: str,
    init_args: InitArgsIllumination,
) -> dict:

    raw_path = init_args.raw_path
    subsets = init_args.subsets
    print("[illumination_correction_subsets] START")
    print(f"[illumination_correction_subsets] {path=}")
    print(f"[illumination_correction_subsets] {raw_path=}")
    print(f"[illumination_correction_subsets] {subsets=}")

    # Prepare output metadata and set actual_path
    if path == raw_path:
        out = dict(image_list_updates=[dict(path=path)])
    else:
        out = dict(image_list_updates=[dict(path=path, origin=raw_path)])
        print(f"[illumination_correction_subsets] {path=}")

    with (Path(path) / "data").open("a") as f:
        f.write(
            f"[illumination_correction_subsets] Running with {raw_path=}, "
            f"{subsets=}\n"
        )

    print("[illumination_correction_subsets] END")
    return out
