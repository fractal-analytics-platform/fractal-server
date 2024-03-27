from pydantic.decorator import validate_arguments
from utils import _extract_common_root
from utils import _group_paths_by_well


def _read_acquisition_index_from_ngff_metadata(path: str) -> int:
    """
    This is a mock, where we simply guess the acquisition index
    from the path. The actual function should read OME-NGFF metadata.
    """
    return int(path.split("/")[-1][0])


@validate_arguments
def calculate_registration_init(
    *,
    paths: list[str],
    zarr_dir: str,
    ref_acquisition: int,
) -> dict:
    """
    Dummy task description.
    """

    print("[calculate_registration_init] START")
    print(f"[calculate_registration_init] {paths=}")

    # Detect plate prefix
    shared_plate = _extract_common_root(paths).get("shared_plate")
    print(f"[calculate_registration_init] Identified {shared_plate=}")
    print(f"[calculate_registration_init] Identified {zarr_dir=}")

    well_to_paths = _group_paths_by_well(paths)

    parallelization_list = []
    for well, well_paths in well_to_paths.items():
        print(f"[calculate_registration_init] {well=}")
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
                    init_args=dict(ref_path=ref_path),
                )
            )

    print("[calculate_registration_init] END")
    return dict(parallelization_list=parallelization_list)


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=calculate_registration_init)
