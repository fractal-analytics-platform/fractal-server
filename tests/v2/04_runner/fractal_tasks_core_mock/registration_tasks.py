from pathlib import Path
from typing import Optional

from pydantic.decorator import validate_arguments

from .utils import _extract_common_root
from fractal_server.app.runner.v2.models import DictStrAny


def _read_acquisition_index_from_ngff_metadata(path: str) -> int:
    """
    This is a mock, where we simply guess the acquisition index
    from the path. The actual function should read OME-NGFF metadata.
    """
    return int(path.split("/")[-1][0])


def _group_paths_by_well(paths: list[str]) -> dict[str, list[str]]:
    """
    Given a list of paths, apply custom logic to group them by well.
    """
    shared_plate = _extract_common_root(paths).get("shared_plate")
    shared_root_dir = _extract_common_root(paths).get("shared_root_dir")
    well_to_paths = {}
    for path in paths:
        # Extract well ID
        relative_path = path.replace(
            f"{shared_root_dir}/{shared_plate}", ""
        ).lstrip("/")
        path_parts = relative_path.split("/")
        well = "/".join(path_parts[0:2])
        # Append to the existing list (or create a new one)
        if well in well_to_paths.keys():
            well_to_paths[well].append(path)
        else:
            well_to_paths[well] = [path]
    return well_to_paths


@validate_arguments
def init_registration(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    ref_acquisition: int,
) -> dict:

    print("[init_registration] START")
    print(f"[init_registration] {paths=}")

    # Detect plate prefix
    shared_plate = _extract_common_root(paths).get("shared_plate")
    shared_root_dir = _extract_common_root(paths).get("shared_root_dir")
    print(f"[init_registration] Identified {shared_plate=}")
    print(f"[init_registration] Identified {shared_root_dir=}")

    well_to_paths = _group_paths_by_well(paths)

    parallelization_list = []
    for well, well_paths in well_to_paths.items():
        print(f"[init_registration] {well=}")
        x_cycles = []
        ref_path = None

        # Loop over all well images, and find the reference one
        for path in well_paths:
            if (
                _read_acquisition_index_from_ngff_metadata(path)
                == ref_acquisition
            ):
                if ref_path is not None:
                    raise ValueError("We should have not reached this branch.")
                ref_path = path
            else:
                x_cycles.append(path)

        # Then, include all actually-relevant (path, ref_path) pairs
        for path in x_cycles:
            parallelization_list.append(
                dict(
                    path=path,
                    ref_path=ref_path,
                )
            )

    print("[init_registration] END")
    return dict(parallelization_list=parallelization_list)


@validate_arguments
def calculate_registration(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    ref_path: str,
) -> dict:
    print("[calculate_registration] START")
    print(f"[calculate_registration] {path=}")
    print(f"[calculate_registration] {ref_path=}")

    table_path = Path(path) / "registration_table"
    print(f"[calculate_registration] Writing to {table_path.as_posix()}")

    with table_path.open("w") as f:
        f.write("Calculate registration for\n" f"{path=}\n" f"{ref_path=}\n")
    print("[calculate_registration] END")


@validate_arguments
def find_registration_consensus(
    *,
    # Standard arguments
    paths: list[str],
    buffer: Optional[DictStrAny] = None,
) -> dict:
    print("[find_registration_consensus] START")
    well_to_paths = _group_paths_by_well(paths)
    for well, well_paths in well_to_paths.items():
        print(f"[find_registration_consensus] {well=}")
        for path in well_paths:
            table_path = Path(path) / "registration_table"
            print(
                f"[find_registration_consensus]   Read {table_path.as_posix()}"
            )
        print(f"[find_registration_consensus] Find consensus for {well=}")
        for path in well_paths:
            table_path = Path(path) / "registration_table_final"
            print(
                "[find_registration_consensus]"
                f"   Write {table_path.as_posix()}"
            )
            with table_path.open("w") as f:
                f.write("This is the consensus-based new table.\n")

    print("[find_registration_consensus] END")


@validate_arguments
def apply_registration_to_image(
    *,
    # Standard arguments
    path: str,
    buffer: Optional[DictStrAny] = None,
    # Non-standard arguments
    overwrite_input: bool = True,
) -> dict:
    print("[registration] START")
    print(f"[registration] {path=}")
    print(f"[registration] {overwrite_input=}")

    table_path = Path(path) / "registration_table_final"
    print(f"[registration] Reading information from {table_path.as_posix()}")
    with table_path.open("r") as f:
        f.read()

    # Handle the case of path=ref_path
    if overwrite_input:
        out = dict(edited_images=[dict(path=path)])
        with (Path(path) / "data").open("a") as f:
            f.write("Applying registration\n")
    else:
        new_path = f"{path}_r"
        print(f"[registration] {new_path=}")
        out = dict(added_images=[dict(path=new_path)])
        Path(new_path).mkdir()
        with (Path(new_path) / "data").open("a") as f:
            f.write("Applying registration\n")
    print(f"[registration] {out=}")
    print("[registration] END")
    return out
