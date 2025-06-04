from fractal_server.app.schemas.v2 import FractalUploadedFile


def collect_local_pixi(
    *,
    task_group_activity_id: int,
    task_group_id: int,
    tar_gz_file: FractalUploadedFile,
) -> None:
    raise NotImplementedError
