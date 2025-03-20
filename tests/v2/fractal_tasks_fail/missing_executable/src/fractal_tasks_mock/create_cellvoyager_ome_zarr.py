from pydantic.decorator import validate_arguments


@validate_arguments
def create_cellvoyager_ome_zarr(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
) -> dict:

    return dict(parallelization_list=[])
