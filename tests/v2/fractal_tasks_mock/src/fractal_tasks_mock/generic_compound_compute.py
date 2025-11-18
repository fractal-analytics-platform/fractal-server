from typing import Optional

from pydantic import validate_call

from fractal_tasks_mock.input_models import InitArgsGeneric


@validate_call
def generic_compound_compute(
    *,
    zarr_url: str,
    init_args: InitArgsGeneric,
    raise_error: bool = False,
    raise_error_if_ind_is_even: bool = False,
) -> Optional[dict]:
    """
    Dummy task description.

    Args:
        zarr_url: description
        init_args: description
        another_argument: description
        raise_error: description
        raise_error_if_ind_is_even: description
    """

    argument = init_args.argument
    print("[generic_compound_compute] START")
    print(f"[generic_compound_compute] {zarr_url=}")
    print(f"[generic_compound_compute] {argument=}")
    print(f"[generic_compound_compute] {raise_error=}")
    print(f"[generic_compound_compute] {raise_error_if_ind_is_even=}")
    if raise_error:
        raise RuntimeError(f"{raise_error=}")
    if raise_error_if_ind_is_even and (init_args.ind % 2 == 0):
        raise RuntimeError(
            f"{init_args.ind=} and {raise_error_if_ind_is_even=}"
        )

    print("[generic_compound_compute] END")
    return None


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=generic_compound_compute)
