from fractal_server.app.models.v2.profile import Profile
from fractal_server.app.models.v2.resource import Resource
from fractal_server.app.schemas.v2.task_collection import FractalUploadedFile


def collect_ssh_apptainer(
    *,
    task_group_id: int,
    task_group_activity_id: int,
    tar_gz_file: FractalUploadedFile,
    resource: Resource,
    profile: Profile,
) -> None: ...
