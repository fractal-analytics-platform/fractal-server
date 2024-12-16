from fractal_server.app.schemas.v2.dataset import _DatasetHistoryItemV2
from fractal_server.app.schemas.v2.workflowtask import WorkflowTaskStatusTypeV2


def test_issue_1783():
    """
    Given a dataset that was processed with legacy tasks from within V2
    workflows, verify that the responds of its GET endpoint is valid.

    The differences
    """

    wftask1 = dict(
        id=1,
        workflow_id=1,
        order=0,
        input_filters=dict(
            attributes_include=dict(), attributes_exclude=dict(), types=dict()
        ),
        task_id=1,
        task=dict(
            id=1,
            name="name",
            type="parallel",
            source="source",
            input_types={},
            output_types={},
        ),
    )
    history_item_1 = dict(
        workflowtask=wftask1,
        status=WorkflowTaskStatusTypeV2.FAILED,
        parallelization=dict(),
    )
    _DatasetHistoryItemV2(**history_item_1)

    wftask2 = dict(
        id=1,
        workflow_id=1,
        order=0,
        input_filters=dict(
            attributes_include=dict(), attributes_exclude=dict(), types=dict()
        ),
        task_legacy_id=1,
        task_legacy=dict(
            id=1,
            input_type="image",
            output_type="zarr",
            command="echo",
            name="name",
            source="source",
        ),
    )
    history_item_2 = dict(
        workflowtask=wftask2,
        status=WorkflowTaskStatusTypeV2.FAILED,
        parallelization=dict(),
    )
    _DatasetHistoryItemV2(**history_item_2)
