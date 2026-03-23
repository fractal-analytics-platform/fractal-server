import json
from typing import Any

from pydantic import validate_call


@validate_call
def create_images(
    *,
    zarr_dir: str,
    num_images: int = 2,
) -> dict[str, Any]:
    """
    Task description

    Args:
        zarr_dir: Description of `zarr_dir`
        num_images: Description of `num_images`
    """
    print("[create_images] START")
    print(f"[create_images] {zarr_dir}")
    print(f"[create_images] {num_images=}")
    zarr_dir = zarr_dir.rstrip("/")
    output = dict(
        image_list_updates=[
            dict(zarr_url=f"{zarr_dir}/{ind}") for ind in range(num_images)
        ]
    )
    print(f"[create_images] {json.dumps(output)}")
    print("[create_images] END")
    return output


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=create_images)
