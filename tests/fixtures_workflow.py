import pytest

from fractal_server.app.models import Subtask
from fractal_server.app.models import Task


LEN_NONTRIVIAL_WORKFLOW = 3


@pytest.fixture
def nontrivial_workflow():
    workflow = Task(
        name="outer workflow",
        resource_type="workflow",
        subtask_list=[
            Subtask(
                subtask=Task(
                    name="inner workflow",
                    resource_type="workflow",
                    subtask_list=[
                        Subtask(
                            args={"message": "dummy0"},
                            subtask=Task(
                                name="dummy0",
                                module="fractal_tasks_core.dummy:dummy",
                                default_args=dict(
                                    message="dummy0", executor="cpu-low"
                                ),
                            ),
                        ),
                        Subtask(
                            args={"message": "dummy1"},
                            subtask=Task(
                                name="dummy1",
                                module="fractal_tasks_core.dummy:dummy",
                                default_args=dict(
                                    message="dummy1", executor="cpu-low"
                                ),
                            ),
                        ),
                    ],
                ),
            ),
            Subtask(
                subtask=Task(
                    name="dummy2",
                    module="fractal_tasks_core.dummy:dummy",
                    default_args=dict(message="dummy2", executor="cpu-low"),
                )
            ),
        ],
    )
    return workflow
