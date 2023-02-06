import json

from devtools import debug

from fractal_server.app.models import WorkflowRead
from fractal_server.app.models import WorkflowUpdate

PREFIX = "/api/v1/project"


async def test_import_workflow(
    client,
    MockCurrentUser,
    project_factory,
    dataset_factory,
    workflow_factory,
    job_factory,
    tmp_path,
    testdata_path,
    collect_packages,
):

    with (
        testdata_path
        / "objects_for_db_import_export"
        / "exported_workflow.json"
    ).open("r") as f:
        wf_payload = json.load(f)
    debug(wf_payload)

    async with MockCurrentUser(persist=True) as user:
        prj = await project_factory(user)

        wf_payload["project_id"] = prj.id

        debug(WorkflowUpdate(**wf_payload))

        res = await client.post(
            f"{PREFIX}/import-workflow",
            # f"{PREFIX}/{prj.id}/import-workflow",
            json=wf_payload,
        )
        debug(res)
        debug(res.json())
        assert False
        debug(WorkflowRead(**res.json()))
        assert res.status_code == 201
