def validate_wft_args(value: dict) -> dict:
    RESERVED_ARGUMENTS = {"zarr_dir", "zarr_url", "zarr_urls", "init_args"}
    args_keys = set(value.keys())
    intersect_keys = RESERVED_ARGUMENTS.intersection(args_keys)
    if intersect_keys:
        raise ValueError(
            f"`args` contains the following forbidden keys: {intersect_keys}"
        )
    return value
