from pydantic import validate_call


@validate_call
def create_cellvoyager_ome_zarr(
    *,
    zarr_urls: list[str],
    zarr_dir: str,
) -> dict:

    return dict(parallelization_list=[])
