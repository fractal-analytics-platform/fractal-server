from .non_parallel_tasks import create_ome_zarr
from .non_parallel_tasks import create_ome_zarr_multiplex
from .non_parallel_tasks import init_channel_parallelization
from .non_parallel_tasks import init_registration
from .non_parallel_tasks import new_ome_zarr
from .parallel_tasks import cellpose_segmentation
from .parallel_tasks import copy_data
from .parallel_tasks import illumination_correction
from .parallel_tasks import maximum_intensity_projection
from .parallel_tasks import registration
from .parallel_tasks import yokogawa_to_zarr
from fractal_server.app.runner.v2.models import Task


TASK_LIST = {
    "create_ome_zarr": Task(
        function=create_ome_zarr, task_type="non_parallel"
    ),
    "yokogawa_to_zarr": Task(function=yokogawa_to_zarr, task_type="parallel"),
    "create_ome_zarr_multiplex": Task(
        function=create_ome_zarr_multiplex, task_type="non_parallel"
    ),
    "cellpose_segmentation": Task(
        function=cellpose_segmentation, task_type="parallel"
    ),
    "new_ome_zarr": Task(function=new_ome_zarr, task_type="non_parallel"),
    "copy_data": Task(function=copy_data, task_type="parallel"),
    "illumination_correction": Task(
        function=illumination_correction,
        task_type="parallel",
        new_filters=dict(illumination_correction=True),
    ),
    "maximum_intensity_projection": Task(
        function=maximum_intensity_projection,
        task_type="parallel",
        new_filters=dict(data_dimensionality="2"),
    ),
    "init_channel_parallelization": Task(
        function=init_channel_parallelization, task_type="non_parallel"
    ),
    "init_registration": Task(
        function=init_registration, task_type="non_parallel"
    ),
    "registration": Task(
        function=registration,
        task_type="parallel",
        new_filters=dict(registration=True),
    ),
}
