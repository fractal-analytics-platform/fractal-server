from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.dataset import Dataset
from fractal_server.app.models.job import ApplyWorkflow
from fractal_server.app.models.workflow import Workflow
from fractal_server.app.schemas.workflow import WorkflowRead


with next(get_sync_db()) as db:

    stm = select(ApplyWorkflow)

    applyworkflows = db.execute(stm)

    rows = applyworkflows.scalars().all()

    for row in rows:

        # get correspondent workflow, input_dataset, output_dataset
        workflow = db.get(Workflow, row.workflow_id)

        input_dataset = db.get(Dataset, row.input_dataset_id)

        output_dataset = db.get(Dataset, row.output_dataset_id)

        # build correct dumps using the same method used in
        # fractal_server.app.routes.api.v1.project
        input_dataset_dump = dict(
            input_dataset.dict(exclude={"resource_list"}),
            resource_list=[
                resource.dict() for resource in input_dataset.resource_list
            ],
        )
        output_dataset_dump = dict(
            output_dataset.dict(exclude={"resource_list"}),
            resource_list=[
                resource.dict() for resource in output_dataset.resource_list
            ],
        )
        workflow_dump = dict(
            workflow.dict(exclude={"task_list"}),
            task_list=[
                dict(wf_task.dict(exclude={"task"}), task=wf_task.task.dict())
                for wf_task in workflow.task_list
            ],
        )

        WorkflowRead(**workflow_dump)

        row.workflow_dump = workflow_dump

        row.input_dataset_dump = input_dataset_dump

        row.output_dataset_dump = output_dataset_dump

        db.add(row)
        db.commit()
