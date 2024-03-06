from pathlib import Path
from typing import Optional

from pydantic.decorator import validate_arguments

from .utils import _extract_common_root
from fractal_server.app.runner.v2.models import DictStrAny


@validate_arguments
def init_registration_old(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
    zarr_dir: str,
    # Non-standard arguments
    ref_cycle_name: str,
) -> dict:

    print("[init_registration] START")
    print(f"[init_registration] {paths=}")

    # Detect plate prefix
    shared_plate = _extract_common_root(paths).get("shared_plate")
    shared_root_dir = _extract_common_root(paths).get("shared_root_dir")
    print(f"[init_registration] Identified {shared_plate=}")
    print(f"[init_registration] Identified {shared_root_dir=}")

    ref_cycles_per_well = {}
    x_cycles_per_well = {}
    wells = []
    for path in paths:
        relative_path = path.replace(
            f"{shared_root_dir}/{shared_plate}", ""
        ).lstrip("/")
        path_parts = relative_path.split("/")
        well = "/".join(path_parts[0:2])
        image = path_parts[2]
        wells.append(well)
        if image == ref_cycle_name:
            if well in ref_cycles_per_well.keys():
                raise ValueError("We should have not reached this branch.")
            ref_cycles_per_well[well] = path
        else:
            cycles = x_cycles_per_well.get(well, [])
            cycles.append(path)
            x_cycles_per_well[well] = cycles

    parallelization_list = []
    for well in sorted(set(wells)):
        print(f"[init_registration] {well=}")
        ref_path = ref_cycles_per_well[well]

        # First, include a dummy pair of paths (ref_path, ref_path)
        parallelization_list.append(dict(path=ref_path, ref_path=ref_path))

        # Then, include all actually-relevant (path, ref_path) pairs
        for path in x_cycles_per_well[well]:
            parallelization_list.append(
                dict(
                    path=path,
                    ref_path=ref_path,
                )
            )

    print("[init_registration] END")
    return dict(parallelization_list=parallelization_list)


@validate_arguments
def registration_old(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    ref_path: str,
    overwrite_input: bool = True,
) -> dict:
    print("[registration] START")
    print(f"[registration] {path=}")
    print(f"[registration] {overwrite_input=}")

    # Handle the case of path=ref_path
    if overwrite_input:
        out = dict(edited_images=[dict(path=path)])
        with (Path(path) / "data").open("a") as f:
            f.write(f"registration against {ref_path=}\n")
    else:
        new_path = f"{path}_r"
        print(f"[registration] {new_path=}")
        out = dict(added_images=[dict(path=new_path)])
        Path(new_path).mkdir()
        with (Path(new_path) / "data").open("a") as f:
            f.write(f"registration against {ref_path=}\n")
    print(f"[registration] {out=}")
    print("[registration] END")
    return out
