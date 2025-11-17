from pathlib import Path

from pydantic import validate_call


@validate_call
def illumination_correction_init(
    *,
    zarr_urls: list[str],
    overwrite_input: bool = False,
    zarr_dir: str,
) -> dict:
    """
    Dummy task description.

    Args:
        zarr_urls: description
        zarr_dir: description
        overwrite_input: Whether to overwrite the current image
    """

    prefix = "[illumination_correction_init]"
    print(f"{prefix} START")
    print(f"{prefix} {zarr_urls=}")
    print(f"{prefix} {overwrite_input=}")

    parallelization_list = []
    for zarr_url in zarr_urls:
        # Create new zarr image if needed
        if not overwrite_input:
            new_zarr_url = f"{zarr_url}_corr"
            Path(new_zarr_url).mkdir()
            with (Path(new_zarr_url) / "data").open("w") as f:
                f.write(
                    f"{prefix} Creating current zarr ({overwrite_input=})\n"
                )
        else:
            new_zarr_url = zarr_url

        # Find out number of channels, from Zarr
        # array shape or from NGFF metadata
        num_channels = 2  # mock
        for ind_channel in range(num_channels):
            parallelization_list.append(
                dict(
                    zarr_url=new_zarr_url,
                    init_args=dict(
                        raw_zarr_url=zarr_url,
                        subsets=dict(C_index=ind_channel),
                    ),
                )
            )
    print(f"{prefix} END")
    return dict(parallelization_list=parallelization_list)


if __name__ == "__main__":
    from fractal_task_tools.task_wrapper import run_fractal_task

    run_fractal_task(task_function=illumination_correction_init)
