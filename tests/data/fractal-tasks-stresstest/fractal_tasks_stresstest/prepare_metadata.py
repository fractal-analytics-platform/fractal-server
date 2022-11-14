import logging
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def prepare_metadata(
    *,
    input_paths: Sequence[Path],
    output_path: Path,
    metadata: Dict[str, Any],
    # Task-specific arguments
    num_components: Optional[int] = 10,
) -> Dict[str, Any]:

    logger.info("ENTERING prepare_metadata task")
    list_components = map(str, list(range(num_components)))
    metadata_update = {"component": list_components}
    logger.info("EXITING dummy task")
    return metadata_update


if __name__ == "__main__":
    from pydantic import BaseModel
    from fractal_tasks_stresstest._utils import run_fractal_task

    class TaskArguments(BaseModel):
        input_paths: Sequence[Path]
        output_path: Path
        metadata: Dict[str, Any]
        num_components: Optional[int]

    run_fractal_task(
        task_function=prepare_metadata, TaskArgsModel=TaskArguments
    )
