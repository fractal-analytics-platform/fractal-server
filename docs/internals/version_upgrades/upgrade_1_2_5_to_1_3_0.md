# Upgrade from 1.2.5 to 1.3.0

A large part of endpoints were updated, mostly to move the foreign-key IDs from
the Pydantic models used to validate the request payload to the endpoint path.
When necessary, some of those same foreign-key IDs are now passed as query
parameters (rather than body or path parameters).
The rationale behind this refactor is that endpoints paths are now more
consistent:

* They have a hierarchical structure, when appropriate (e.g. a project may
  contain a datasets, workflows and jobs, while a workflow may contain
  workflowtasks, ...).
* They are more consistently defined and more transparently related to CRUD
  operations (see e.g.
  [here](https://github.com/fractal-analytics-platform/fractal-server/issues/550#issuecomment-1537963980)).


The list of updated endpoints is below. Note that the IDs that are now part of
the path/query parameters are not required any more as part of the body
parameters.

```
OLD GET /api/v1/job/download/{job_id}
NEW GET /api/v1/project/{project_id}/job/{job_id}/download/

OLD GET /api/v1/job/{job_id}
NEW GET /api/v1/project/{project_id}/job/{job_id}

OLD GET /api/v1/project/{project_id}/jobs/
NEW GET /api/v1/project/{project_id}/job/

OLD POST /api/v1/project/{project_id}/{dataset_id}
NEW POST /api/v1/project/{project_id}/dataset/{dataset_id}/resource/

OLD GET /api/v1/project/{project_id}/{dataset_id}
NEW GET /api/v1/project/{project_id}/dataset/{dataset_id}

OLD GET /api/v1/project/{project_id}/{dataset_id}/resources/
NEW GET /api/v1/project/{project_id}/dataset/{dataset_id}/resource/

OLD PATCH /api/v1/project/{project_id}/{dataset_id}
NEW PATCH /api/v1/project/{project_id}/dataset/{dataset_id}

OLD PATCH /api/v1/project/{project_id}/{dataset_id}/{resource_id}
NEW PATCH /api/v1/project/{project_id}/dataset/{dataset_id}/resource/{resource_id}

OLD DELETE /api/v1/project/{project_id}/{dataset_id}
NEW DELETE /api/v1/project/{project_id}/dataset/{dataset_id}

OLD DELETE /api/v1/project/{project_id}/{dataset_id}/{resource_id}
NEW DELETE /api/v1/project/{project_id}/dataset/{dataset_id}/resource/{resource_id}

OLD PATCH /api/v1/workflow/{workflow_id}/edit-task/{workflow_task_id}
NEW PATCH /api/v1/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}

OLD DELETE /api/v1/workflow/{workflow_id}/rm-task/{workflow_task_id}
NEW DELETE /api/v1/project/{project_id}/workflow/{workflow_id}/wftask/{workflow_task_id}

OLD POST /api/v1/workflow/{workflow_id}/add-task/
NEW POST /api/v1/project/{project_id}/workflow/{workflow_id}/wftask/
NEW QUERY PARAMETERS: ['task_id']

OLD POST /api/v1/project/{project_id}/import-workflow/
NEW POST /api/v1/project/{project_id}/workflow/import/

OLD POST /api/v1/project/apply/
NEW POST /api/v1/project/{project_id}/workflow/{workflow_id}/apply/
NEW QUERY PARAMETERS: ['input_dataset_id', 'output_dataset_id']

OLD POST /api/v1/workflow/
NEW POST /api/v1/project/{project_id}/workflow/

OLD GET /api/v1/project/{project_id}/workflows/
NEW GET /api/v1/project/{project_id}/workflow/

OLD GET /api/v1/workflow/{workflow_id}
NEW GET /api/v1/project/{project_id}/workflow/{workflow_id}

OLD GET /api/v1/workflow/{workflow_id}/export/
NEW GET /api/v1/project/{project_id}/workflow/{workflow_id}/export/

OLD PATCH /api/v1/workflow/{workflow_id}
NEW PATCH /api/v1/project/{project_id}/workflow/{workflow_id}

OLD DELETE /api/v1/workflow/{workflow_id}
NEW DELETE /api/v1/project/{project_id}/workflow/{workflow_id}

OLD POST /api/v1/project/{project_id}/
NEW POST /api/v1/project/{project_id}/dataset/

NEW GET /api/v1/project/{project_id}/job/{job_id}/stop/
```
