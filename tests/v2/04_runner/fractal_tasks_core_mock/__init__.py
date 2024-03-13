from .illumination_correction import illumination_correction
from .illumination_correction import illumination_correction_B
from .illumination_correction import init_channel_parallelization
from .non_parallel_tasks import create_ome_zarr
from .non_parallel_tasks import create_ome_zarr_multiplex
from .non_parallel_tasks import new_ome_zarr
from .parallel_tasks import cellpose_segmentation
from .parallel_tasks import maximum_intensity_projection
from .parallel_tasks import yokogawa_to_zarr
from .registration_tasks import apply_registration_to_image
from .registration_tasks import calculate_registration
from .registration_tasks import find_registration_consensus
from .registration_tasks import init_registration
from fractal_server.app.runner.v2.models import Task


TASK_LIST = {
    "create_ome_zarr_compound": Task(
        function_non_parallel=create_ome_zarr,
        function_parallel=yokogawa_to_zarr,
    ),
    "MIP_compound": Task(
        function_non_parallel=new_ome_zarr,
        function_parallel=maximum_intensity_projection,
    ),
    "illumination_correction": Task(
        function_parallel=illumination_correction,
        new_filters=dict(illumination_correction=True),
    ),
    "cellpose_segmentation": Task(
        function_parallel=cellpose_segmentation,
    ),
    "create_ome_zarr_multiplex": Task(
        function_non_parallel=create_ome_zarr_multiplex,
    ),
    "illumination_correction_compound": Task(
        function_non_parallel=init_channel_parallelization,
        function_parallel=illumination_correction_B,
        new_filters=dict(illumination_correction=True),
    ),
    # Block of new mocks for registration tasks
    "registration_part_1_compound": Task(
        function_non_parallel=init_registration,
        function_parallel=calculate_registration,
    ),
    "registration_part_2_compound": Task(
        function_non_parallel=find_registration_consensus,
        function_parallel=apply_registration_to_image,
        new_filters=dict(registration=True),
    ),
}
