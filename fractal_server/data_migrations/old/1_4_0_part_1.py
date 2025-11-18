from sqlalchemy import select

from fractal_server.app.db import get_sync_db
from fractal_server.app.models.v1.dataset import Dataset
from fractal_server.app.models.v1.job import ApplyWorkflow
from fractal_server.app.models.v1.workflow import Workflow
from fractal_server.app.schemas.v1.workflow import WorkflowReadV1


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
            input_dataset.model_dump(exclude={"resource_list"}),
            resource_list=[
                resource.model_dump()
                for resource in input_dataset.resource_list
            ],
        )
        output_dataset_dump = dict(
            output_dataset.model_dump(exclude={"resource_list"}),
            resource_list=[
                resource.model_dump()
                for resource in output_dataset.resource_list
            ],
        )
        workflow_dump = dict(
            workflow.model_dump(exclude={"task_list"}),
            task_list=[
                dict(
                    wf_task.model_dump(exclude={"task"}),
                    task=wf_task.task.model_dump(),
                )
                for wf_task in workflow.task_list
            ],
        )

        WorkflowReadV1(**workflow_dump)

        row.workflow_dump = workflow_dump

        row.input_dataset_dump = input_dataset_dump

        row.output_dataset_dump = output_dataset_dump

        db.add(row)
        db.commit()
