from pathlib import Path

from fractal_tasks_mock.input_models import InitArgsRegistration
from pydantic import validate_call


@validate_call
def calculate_registration_compute(
    *,
    zarr_url: str,
    init_args: InitArgsRegistration,
) -> None:
    """
    Dummy task description.

    Args:
        path: description
        init_args: description
    """

    ref_zarr_url = init_args.ref_zarr_url
    print("[calculate_registration_compute] START")
    print(f"[calculate_registration_compute] {zarr_url=}")
    print(f"[calculate_registration_compute] {ref_zarr_url=}")

    table_path = Path(zarr_url) / "registration_table"
    print(
        f"[calculate_registration_compute] Writing to {table_path.as_posix()}"
    )

    with table_path.open("w") as f:
        f.write(f"Calculate registration for\n{zarr_url=}\n{ref_zarr_url=}\n")
    print("[calculate_registration_compute] END")


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=calculate_registration_compute)
