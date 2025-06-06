from fractal_server.app.schemas.v2 import FractalUploadedFile
from fractal_server.ssh._fabric import SSHConfig


def collect_ssh_pixi(
    *,
    task_group_id: int,
    task_group_activity_id: int,
    ssh_config: SSHConfig,
    tasks_base_dir: str,
    tar_gz_file: FractalUploadedFile,
) -> None:
    raise NotImplementedError
