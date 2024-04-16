from copy import deepcopy
from pathlib import Path
from typing import Any


def convert_v2_args_into_v1(
    kwargs_v2: dict[str, Any],
    parallelization_level: str = "image",
) -> dict[str, Any]:
    kwargs_v1 = deepcopy(kwargs_v2)

    zarr_url = kwargs_v1.pop("zarr_url")
    input_path = Path(zarr_url).parents[3].as_posix()
    image_component = zarr_url.replace(input_path, "").lstrip("/")
    if parallelization_level == "image":
        component = image_component
    elif parallelization_level == "well":
        component = str(Path(image_component).parent)
    elif parallelization_level == "plate":
        component = str(Path(image_component).parents[2])
    else:
        raise ValueError(f"Invalid {parallelization_level=}.")

    kwargs_v1.update(
        input_paths=[input_path],
        output_path=input_path,
        metadata={},
        component=component,
    )

    return kwargs_v1
