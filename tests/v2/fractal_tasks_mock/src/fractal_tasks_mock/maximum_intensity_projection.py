import shutil

from input_models import InitArgsMIP
from pydantic.decorator import validate_arguments


@validate_arguments
def maximum_intensity_projection(
    *,
    path: str,
    init_args: InitArgsMIP,
) -> dict:
    """
    Dummy task description.

    Arguments:
        path: dummy argument description.
        init_args: dummy argument description.
    """

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


if __name__ == "__main__":
    from utils import run_fractal_task

    run_fractal_task(task_function=maximum_intensity_projection)
