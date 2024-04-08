from pathlib import Path

from fractal_server.images import SingleImage


def image_data_exist_on_disk(image_list: list[SingleImage]):
    """
    Given an image list, check whether mock data were written to disk.
    """
    prefix = "[image_data_exist_on_disk]"
    all_images_have_data = True
    for image in image_list:
        if (Path(image.zarr_url) / "data").exists():
            print(f"{prefix} {image.zarr_url} contains data")
        else:
            print(f"{prefix} {image.zarr_url} does *not* contain data")
            all_images_have_data = False
    return all_images_have_data
