import os
from typing import Optional

from fractal_server.app.runner.v2.models import DictStrAny


def _extract_common_root(paths: list[str]) -> dict[str, str]:
    shared_plates = []
    shared_root_dirs = []
    for path in paths:
        tmp = path.split(".zarr/")[0]
        shared_root_dirs.append("/".join(tmp.split("/")[:-1]))
        shared_plates.append(tmp.split("/")[-1] + ".zarr")

    if len(set(shared_plates)) > 1 or len(set(shared_root_dirs)) > 1:
        raise ValueError
    shared_plate = list(shared_plates)[0]
    shared_root_dir = list(shared_root_dirs)[0]

    return dict(shared_root_dir=shared_root_dir, shared_plate=shared_plate)


def _check_path_is_absolute(_path: str) -> None:
    if not os.path.isabs(_path):
        raise ValueError(f"Path is not absolute ({_path}).")


def _check_buffer_is_empty(_buffer: Optional[DictStrAny] = None) -> None:
    if _buffer not in [None, {}]:
        raise ValueError(
            "Error: `buffer` must be None or empty, but " f"buffer={_buffer}."
        )
