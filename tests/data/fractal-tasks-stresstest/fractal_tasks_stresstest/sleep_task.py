import logging
import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def sleep_task(
    *,
    input_paths: Sequence[Path],
    output_path: Path,
    metadata: Dict[str, Any],
    component: str,
    # Task-specific arguments
    sleep_time: Optional[float] = 1.0,
) -> Dict[str, Any]:
    """
    :param sleep_time: Number of components to be added to metadata
    """

    logger.info("ENTERING sleep_task")
    time.sleep(sleep_time)
    logger.info("EXITING sleep_task")

    return {}


if __name__ == "__main__":
    from pydantic import BaseModel
    from fractal_tasks_stresstest._utils import run_fractal_task

    class TaskArguments(BaseModel):
        input_paths: Sequence[Path]
        output_path: Path
        metadata: Dict[str, Any]
        component: str
        sleep_time: Optional[float]

    run_fractal_task(task_function=sleep_task, TaskArgsModel=TaskArguments)
