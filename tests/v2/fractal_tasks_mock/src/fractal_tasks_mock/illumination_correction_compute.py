from pathlib import Path
from typing import Optional

from pydantic import validate_call

from fractal_tasks_mock.input_models import InitArgsIllumination


@validate_call
def illumination_correction_compute(
    *,
    zarr_url: str,
    init_args: InitArgsIllumination,
    another_argument: str,
) -> Optional[dict]:
    """
    Dummy task description.

    Args:
        zarr_url: description
        init_args: description
    """

    raw_zarr_url = init_args.raw_zarr_url
    subsets = init_args.subsets
    print("[illumination_correction_compute] START")
    print(f"[illumination_correction_compute] {zarr_url=}")
    print(f"[illumination_correction_compute] {raw_zarr_url=}")
    print(f"[illumination_correction_compute] {subsets=}")
    print(f"[illumination_correction_compute] {another_argument=}")

    # Prepare output metadata
    if zarr_url == raw_zarr_url:
        out = None
    else:
        out = dict(
            image_list_updates=[dict(zarr_url=zarr_url, origin=raw_zarr_url)]
        )
        print(f"[illumination_correction_compute] {zarr_url=}")

    with (Path(zarr_url) / "data").open("a") as f:
        f.write(
            f"[illumination_correction_compute] Running with {raw_zarr_url=}, "
            f"{subsets=}\n"
        )

    print("[illumination_correction_compute] END")
    return out


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=illumination_correction_compute)
