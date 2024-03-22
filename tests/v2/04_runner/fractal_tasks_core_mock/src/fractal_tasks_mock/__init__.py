from .apply_registration_to_image import apply_registration_to_image
from .calculate_registration_compute import calculate_registration_compute
from .calculate_registration_init import calculate_registration_init
from .cellpose_segmentation import cellpose_segmentation
from .create_cellvoyager_ome_zarr import create_cellvoyager_ome_zarr
from .create_cellvoyager_ome_zarr_multiplex import (
    create_cellvoyager_ome_zarr_multiplex,
)
from .fill_cellvoyager_ome_zarr import fill_cellvoyager_ome_zarr
from .find_registration_consensus import find_registration_consensus
from .illumination_correction import illumination_correction
from .illumination_correction_channel_parallelization import (
    illumination_correction_channel_parallelization,
)
from .illumination_correction_subsets import illumination_correction_subsets
from .maximum_intensity_projection import maximum_intensity_projection
from .new_ome_zarr import new_ome_zarr
from fractal_server.app.runner.v2.models import Task


TASK_LIST = {
    "create_ome_zarr_compound": Task(
        name="create_ome_zarr_compound",
        function_non_parallel=create_cellvoyager_ome_zarr,
        function_parallel=fill_cellvoyager_ome_zarr,
    ),
    "create_ome_zarr_multiplex_compound": Task(
        name="create_ome_zarr_multiplex_compound",
        function_non_parallel=create_cellvoyager_ome_zarr_multiplex,
        function_parallel=fill_cellvoyager_ome_zarr,
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
        function_non_parallel=illumination_correction_channel_parallelization,
        function_parallel=illumination_correction_subsets,
        output_types=dict(illumination_correction=True),
    ),
    "cellpose_segmentation": Task(
        name="cellpose_segmentation",
        function_parallel=cellpose_segmentation,
    ),
    "calculate_registration_compound": Task(
        name="calculate_registration_compound",
        function_non_parallel=calculate_registration_init,
        function_parallel=calculate_registration_compute,
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
