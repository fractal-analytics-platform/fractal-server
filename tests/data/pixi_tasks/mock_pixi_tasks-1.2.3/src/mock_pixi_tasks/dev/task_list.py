from fractal_task_tools.task_models import ConverterNonParallelTask

TASK_LIST = [
    ConverterNonParallelTask(
        name="create_images",
        executable="create_images.py",
    ),
]
