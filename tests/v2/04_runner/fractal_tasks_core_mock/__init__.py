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
        name="create_ome_zarr_compound",
        function_non_parallel=create_ome_zarr,
        function_parallel=yokogawa_to_zarr,
    ),
    "create_ome_zarr_multiplex_compound": Task(
        name="create_ome_zarr_multiplex_compound",
        function_non_parallel=create_ome_zarr_multiplex,
        function_parallel=yokogawa_to_zarr,
    ),
    "MIP_compound": Task(
        name="MIP_compound",
        input_types={"3D": True},
        function_non_parallel=new_ome_zarr,
        function_parallel=maximum_intensity_projection,
        output_types={"3D": False},
    ),
    "illumination_correction": Task(
        name="illumination_correction",
        input_types=dict(illumination_correction=False),
        function_parallel=illumination_correction,
        output_types=dict(illumination_correction=True),
    ),
    "illumination_correction_compound": Task(
        name="illumination_correction_compound",
        input_types=dict(illumination_correction=False),
        function_non_parallel=init_channel_parallelization,
        function_parallel=illumination_correction_B,
        output_types=dict(illumination_correction=True),
    ),
    "cellpose_segmentation": Task(
        name="cellpose_segmentation",
        function_parallel=cellpose_segmentation,
    ),
    "calculate_registration_compound": Task(
        name="calculate_registration_compound",
        function_non_parallel=init_registration,
        function_parallel=calculate_registration,
    ),
    "find_registration_consensus": Task(
        name="find_registration_consensus",
        function_non_parallel=find_registration_consensus,
    ),
    "apply_registration_to_image": Task(
        name="apply_registration_to_image",
        input_types=dict(registration=False),
        function_parallel=apply_registration_to_image,
        output_types=dict(registration=True),
    ),
}
