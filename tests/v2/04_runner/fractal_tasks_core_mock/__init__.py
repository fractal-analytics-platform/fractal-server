from .converters import create_ome_zarr
from .converters import create_ome_zarr_multiplex
from .converters import yokogawa_to_zarr
from .illumination_correction import illumination_correction
from .illumination_correction import illumination_correction_B
from .illumination_correction import init_channel_parallelization
from .mip import maximum_intensity_projection
from .mip import new_ome_zarr
from .parallel_tasks import cellpose_segmentation
from .registration import apply_registration_to_image
from .registration import calculate_registration
from .registration import find_registration_consensus
from .registration import init_registration
from fractal_server.app.runner.v2.models import Task


TASK_LIST = {
    "create_ome_zarr_compound": Task(
        function_non_parallel=create_ome_zarr,
        function_parallel=yokogawa_to_zarr,
    ),
    "create_ome_zarr_multiplex_compound": Task(
        function_non_parallel=create_ome_zarr_multiplex,
        function_parallel=yokogawa_to_zarr,
    ),
    "MIP_compound": Task(
        function_non_parallel=new_ome_zarr,
        function_parallel=maximum_intensity_projection,
    ),
    "illumination_correction": Task(
        function_parallel=illumination_correction,
        new_type_filters=dict(illumination_correction=True),
    ),
    "illumination_correction_compound": Task(
        function_non_parallel=init_channel_parallelization,
        function_parallel=illumination_correction_B,
        new_type_filters=dict(illumination_correction=True),
    ),
    "cellpose_segmentation": Task(
        function_parallel=cellpose_segmentation,
    ),
    "registration_part_1_compound": Task(
        function_non_parallel=init_registration,
        function_parallel=calculate_registration,
    ),
    "find_registration_consensus": Task(
        function_non_parallel=find_registration_consensus,
    ),
    "apply_registration_to_image": Task(
        function_parallel=apply_registration_to_image,
        new_type_filters=dict(registration=True),
    ),
}
