import os


def _extract_common_root(zarr_urls: list[str]) -> dict[str, str]:
    shared_plates = []
    shared_root_dirs = []
    for zarr_url in zarr_urls:
        tmp = zarr_url.split(".zarr/")[0]
        shared_root_dirs.append("/".join(tmp.split("/")[:-1]))
        shared_plates.append(tmp.split("/")[-1] + ".zarr")

    if len(set(shared_plates)) > 1 or len(set(shared_root_dirs)) > 1:
        raise ValueError
    shared_plate = list(shared_plates)[0]
    shared_root_dir = list(shared_root_dirs)[0]

    return dict(shared_root_dir=shared_root_dir, shared_plate=shared_plate)


def _check_zarr_url_is_absolute(_zarr_url: str) -> None:
    if not os.path.isabs(_zarr_url):
        raise ValueError(f"Path is not absolute ({_zarr_url}).")


def _group_zarr_urls_by_well(zarr_urls: list[str]) -> dict[str, list[str]]:
    """
    Given a list of paths, apply custom logic to group them by well.
    """
    shared_plate = _extract_common_root(zarr_urls).get("shared_plate")
    shared_root_dir = _extract_common_root(zarr_urls).get("shared_root_dir")
    well_to_zarr_urls = {}
    for zarr_url in zarr_urls:
        # Extract well ID
        relative_path = zarr_url.replace(
            f"{shared_root_dir}/{shared_plate}", ""
        ).lstrip("/")
        path_parts = relative_path.split("/")
        well = "/".join(path_parts[0:2])
        # Append to the existing list (or create a new one)
        if well in well_to_zarr_urls.keys():
            well_to_zarr_urls[well].append(zarr_url)
        else:
            well_to_zarr_urls[well] = [zarr_url]
    return well_to_zarr_urls
