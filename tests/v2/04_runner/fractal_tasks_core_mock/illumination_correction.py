from pathlib import Path
from typing import Literal
from typing import Optional

from pydantic.decorator import validate_arguments

from .utils import _check_buffer_is_empty
from fractal_server.app.runner.v2.models import DictStrAny


@validate_arguments
def illumination_correction(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    overwrite_input: bool = False,
) -> dict:
    print("[illumination_correction] START")
    print(f"[illumination_correction] {path=}")
    print(f"[illumination_correction] {overwrite_input=}")

    _check_buffer_is_empty(buffer)

    # Prepare output metadata and set actual_path
    if overwrite_input:
        out = dict(edited_images=[dict(path=path)])
        actual_path = path
    else:
        new_path = f"{path}_corr"
        Path(new_path).mkdir(exist_ok=True)
        out = dict(added_images=[dict(path=new_path)])
        actual_path = new_path
        print(f"[illumination_correction] {new_path=}")

    with (Path(actual_path) / "data").open("a") as f:
        f.write(f"[illumination_correction] ({overwrite_input=})\n")

    print("[illumination_correction] END")
    return out


@validate_arguments
def init_channel_parallelization(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
    overwrite_input: bool = False,
) -> dict:

    _check_buffer_is_empty(buffer)

    print("[init_channel_parallelization] START")
    print(f"[init_channel_parallelization] {paths=}")
    print(f"[init_channel_parallelization] {overwrite_input=}")

    parallelization_list = []
    for path in paths:

        # Create new zarr image if needed
        if not overwrite_input:
            new_path = f"{path}_corr"
            Path(new_path).mkdir()
            with (Path(new_path) / "data").open("w") as f:
                f.write(
                    "[init_channel_parallelization] Creating current zarr "
                    f"({overwrite_input=})\n"
                )
        else:
            new_path = path

        # Find out number of channels, from Zarr
        # array shape or from NGFF metadata
        num_channels = 2  # mock
        for ind_channel in range(num_channels):
            parallelization_list.append(
                dict(
                    path=new_path,
                    raw_path=path,
                    subsets=dict(C_index=ind_channel),
                )
            )
    print("[init_channel_parallelization] END")
    return dict(parallelization_list=parallelization_list)


@validate_arguments
def illumination_correction_B(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    raw_path: str,
    subsets: Optional[
        dict[Literal["T_index", "C_index", "Z_index"], int]
    ] = None,
) -> dict:
    print("[illumination_correction_B] START")
    print(f"[illumination_correction_B] {path=}")
    print(f"[illumination_correction_B] {raw_path=}")
    print(f"[illumination_correction_B] {subsets=}")

    _check_buffer_is_empty(buffer)

    # Prepare output metadata and set actual_path
    if path == raw_path:
        out = dict(edited_images=[dict(path=path)])
    else:
        out = dict(added_images=[dict(path=path)])
        print(f"[illumination_correction_B] {path=}")

    with (Path(path) / "data").open("a") as f:
        f.write(
            f"[illumination_correction_B] Running with {raw_path=}, "
            f"{subsets=}\n"
        )

    print("[illumination_correction] END")
    return out
