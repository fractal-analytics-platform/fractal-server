import shutil

from pydantic import BaseModel
from pydantic.decorator import validate_arguments


class InitArgsMIP(BaseModel):
    new_path: str
    new_plate: str  # FIXME: remove this


@validate_arguments
def maximum_intensity_projection(
    *,
    path: str,
    init_args: InitArgsMIP,
) -> dict:

    new_path = init_args.new_path
    new_plate = init_args.new_plate  # FIXME: re-compute it here

    shutil.copytree(path, new_path)

    print("[maximum_intensity_projection] START")
    print(f"[maximum_intensity_projection] {path=}")
    print(f"[maximum_intensity_projection] {new_path=}")
    print("[maximum_intensity_projection] END")

    out = dict(
        image_list_updates=[
            dict(
                path=new_path,
                origin=path,
                attributes=dict(plate=new_plate),
            )
        ],
    )
    return out
