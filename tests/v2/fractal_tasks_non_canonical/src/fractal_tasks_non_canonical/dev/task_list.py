from fractal_task_tools.task_models import NonParallelTask
from fractal_task_tools.task_models import ParallelTask


TASK_LIST = [
    NonParallelTask(
        name="generic_task",
        executable="generic_task.py",
    ),
    ParallelTask(
        name="generic_task_parallel",
        executable="generic_task_parallel.py",
        input_types=dict(my_type=False),
        output_types=dict(my_type=True),
    ),
]


PACKAGE = "fractal_tasks_non_canonical"
AUTHORS = "name1 surname1, name2 surname2"
