from pathlib import Path
from typing import Any


def _convert_v2_args_into_v1(
    kwargs: dict[str, Any], parallelization_level: str
):

    # Remove buffer
    if "buffer" in kwargs.keys():
        kwargs.pop("buffer")

    path = kwargs.pop("path")
    input_path = Path(path).parents[3].as_posix()
    image_component = path.replace(input_path, "").lstrip("/")
    if parallelization_level == "image":
        component = image_component
    elif parallelization_level == "well":
        component = str(Path(image_component).parent)
    elif parallelization_level == "plate":
        component = str(Path(image_component).parents[2])

    kwargs["input_paths"] = [input_path]
    kwargs["output_path"] = input_path  # TBD: is it always like this??
    kwargs["metadata"] = {}
    kwargs["component"] = component
    return kwargs


kwargs_v1 = _convert_v2_args_into_v1(
    dict(path="/invalid/plate.zarr/A/01/0", arg1=1),
    parallelization_level="image",
)
print(kwargs_v1)
kwargs_v1 = _convert_v2_args_into_v1(
    dict(path="/invalid/plate.zarr/A/01/0", arg1=1),
    parallelization_level="well",
)
print(kwargs_v1)
kwargs_v1 = _convert_v2_args_into_v1(
    dict(path="/invalid/plate.zarr/A/01/0", arg1=1),
    parallelization_level="plate",
)
print(kwargs_v1)
