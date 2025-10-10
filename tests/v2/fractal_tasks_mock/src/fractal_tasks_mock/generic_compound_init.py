from pydantic import validate_call


@validate_call
def generic_compound_init(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
    argument: int = 1,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_urls: description
        zarr_dir: description
        argument: description

    """

    prefix = "[generic_compound_init]"
    print(f"{prefix} START")
    print(f"{prefix} {zarr_urls=}")
    print(f"{prefix} {zarr_dir=}")
    print(f"{prefix} {argument=}")
    parallelization_list = [
        dict(
            zarr_url=zarr_url,
            init_args=dict(ind=ind, argument=argument),
        )
        for ind, zarr_url in enumerate(zarr_urls)
    ]
    print(f"{prefix} END")
    return dict(parallelization_list=parallelization_list)


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=generic_compound_init)
