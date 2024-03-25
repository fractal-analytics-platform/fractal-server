from pathlib import Path

from pydantic.decorator import validate_arguments


@validate_arguments
def illumination_correction_init(
    *,
    paths: list[str],
    overwrite_input: bool = False,
    zarr_dir: str,
) -> dict:

    prefix = "[illumination_correction_init]"
    print(f"{prefix} START")
    print(f"{prefix} {paths=}")
    print(f"{prefix} {overwrite_input=}")

    parallelization_list = []
    for path in paths:

        # Create new zarr image if needed
        if not overwrite_input:
            new_path = f"{path}_corr"
            Path(new_path).mkdir()
            with (Path(new_path) / "data").open("w") as f:
                f.write(
                    f"{prefix} Creating current zarr "
                    f"({overwrite_input=})\n"
                )
        else:
            new_path = path

        # Find out number of channels, from Zarr
        # array shape or from NGFF metadata
        num_channels = 2  # mock
        for ind_channel in range(num_channels):
            parallelization_list.append(
                dict(
                    path=new_path,
                    init_args=dict(
                        raw_path=path,
                        subsets=dict(C_index=ind_channel),
                    ),
                )
            )
    print(f"{prefix} END")
    return dict(parallelization_list=parallelization_list)
