from fractal_server.app.schemas.user import UserCreate
from fractal_server.app.schemas.v2 import DatasetImportV2
from fractal_server.app.schemas.v2 import JobCreateV2
from fractal_server.app.schemas.v2 import ProjectCreateV2
from fractal_server.app.schemas.v2 import WorkflowCreateV2
from fractal_server.app.schemas.v2 import WorkflowTaskCreateV2
from scripts.client import FractalClient


def create_image_list(n_images: int) -> list:
    images_list = []
    for index in range(n_images):
        images_list.append(
            {
                "zarr_url": (
                    f"/invalid/zarr/very/very/long/"
                    f"path/to/mimic/real/"
                    f"path/to/the/zarr/dir/{index:06d}"
                ),
                "origin": (
                    f"/invalid/zarr/very/very/very/long/"
                    f"path/to/mimic/real/path/"
                    f"to/the/zarr/dir/origin-{index:06d}"
                ),
                "types": {"is_3D": True},
                "attributes": {
                    "plate": "my-beautiful-plate.zarr",
                    "well": "A99",
                },
            }
        )
    return images_list


def _create_user_client(
    _admin: FractalClient, user_identifier: str
) -> FractalClient:
    email = f"{user_identifier}@example.org"
    password = f"{user_identifier}-pwd"
    project_dir = f"/fake/{user_identifier}"
    _user = _admin.add_user(
        UserCreate(
            email=email,
            password=password,
            project_dir=project_dir,
        )
    )
    _admin.associate_user_with_profile(user_id=_user.id)
    _user = FractalClient(credentials=dict(username=email, password=password))
    print(_user.whoami())
    return _user


# vanilla user:
# 1 project
# 1 dataset with a single resource
# 1 workflow with a single task
# 1 job
def _user_flow_vanilla(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="vanilla")
    proj = user.add_project(ProjectCreateV2(name="MyProject_uv"))
    image_list = create_image_list(n_images=10)
    ds = user.import_dataset(
        proj.id,
        DatasetImportV2(
            name="MyDataset", zarr_dir="/invalid/zarr", images=image_list
        ),
    )
    wf = user.add_workflow(proj.id, WorkflowCreateV2(name="MyWorkflow"))
    user.add_workflowtask(
        proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
    )
    user.submit_job(proj.id, wf.id, ds.id, applyworkflow=JobCreateV2())


# power user:
# 1 project
# `num_jobs_per_workflow` datasets with a single resource
# `num_workflows` workflows, half with 1, half with 3 tasks
# `num_jobs_per_workflow` jobs
def _user_flow_power(
    admin: FractalClient,
    *,
    working_task_id: int,
    failing_task_id: int,
):
    user = _create_user_client(admin, user_identifier="power")
    proj = user.add_project(ProjectCreateV2(name="MyProject_upw"))
    # we add also a dataset with images
    image_list = create_image_list(n_images=100)
    num_workflows = 20
    num_jobs_per_workflow = 20
    for ind_wf in range(num_workflows):
        wf = user.add_workflow(
            proj.id, WorkflowCreateV2(name=f"MyWorkflow-{ind_wf}")
        )
        user.add_workflowtask(
            proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
        )
        if ind_wf % 2 == 0:
            user.add_workflowtask(
                proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
            )
            user.add_workflowtask(
                proj.id, wf.id, failing_task_id, WorkflowTaskCreateV2()
            )
        for ind_job in range(num_jobs_per_workflow):
            ds = user.import_dataset(
                proj.id,
                DatasetImportV2(
                    name="MyDataset",
                    zarr_dir="/invalid/zarr",
                    images=image_list,
                ),
            )
            user.submit_job(
                proj.id,
                wf.id,
                ds.id,
                applyworkflow=JobCreateV2(),
            )


# dataset user:
# 1 project
# `n_datasets` datasets
# `num_workflows` workflows, with a single task
# `n_datasets` jobs
def _user_flow_dataset(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="dataset")
    proj = user.add_project(ProjectCreateV2(name="MyProject_us"))
    image_list = create_image_list(n_images=1000)
    n_datasets = 20
    ds_list = []
    for i in range(n_datasets):
        ds = user.import_dataset(
            proj.id,
            DatasetImportV2(
                name=f"MyDataset_us-{i}",
                zarr_dir="/invalid/zarr",
                images=image_list,
            ),
        )
        ds_list.append(ds)

    num_workflows = 20
    for i in range(num_workflows):
        wf = user.add_workflow(
            proj.id, WorkflowCreateV2(name=f"MyWorkflow_us-{i}")
        )
        user.add_workflowtask(
            proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
        )
        for ds in ds_list:
            user.submit_job(
                proj.id,
                wf.id,
                ds.id,
                applyworkflow=JobCreateV2(),
            )


# project user:
# `n_projects` project
# 2 datasets with a single resource per project
# 1 workflow, with a single task
# `num_jobs_per_workflow` jobs
def _user_flow_project(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="project")
    n_projects = 25
    num_jobs_per_workflow = 5
    image_list = create_image_list(100)
    for i in range(n_projects):
        proj = user.add_project(ProjectCreateV2(name=f"MyProject_upj-{i}"))
        ds = user.import_dataset(
            proj.id,
            DatasetImportV2(
                name=f"MyDataset_up-{i}",
                zarr_dir="/invalid/zarr",
                images=image_list,
            ),
        )
        wf = user.add_workflow(
            proj.id, WorkflowCreateV2(name=f"MyWorkflow_up-{i}")
        )
        user.add_workflowtask(
            proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
        )
        for i in range(num_jobs_per_workflow):
            user.submit_job(
                proj.id,
                wf.id,
                ds.id,
                applyworkflow=JobCreateV2(),
            )


# job user:
# 1 project
# 1 dataset with a single resource
# 1 workflow with a single task
# `num_jobs_per_workflow` job
def _user_flow_job(
    admin: FractalClient,
    working_task_id: int,
):
    user = _create_user_client(admin, user_identifier="job")
    proj = user.add_project(ProjectCreateV2(name="MyProject_uj"))
    image_list = create_image_list(n_images=10)
    ds = user.import_dataset(
        proj.id,
        DatasetImportV2(
            name="MyDataset", zarr_dir="/invalid/zarr", images=image_list
        ),
    )
    wf = user.add_workflow(proj.id, WorkflowCreateV2(name="MyWorkflow_uj"))
    user.add_workflowtask(
        proj.id, wf.id, working_task_id, WorkflowTaskCreateV2()
    )
    num_jobs_per_workflow = 100
    for i in range(num_jobs_per_workflow):
        user.submit_job(proj.id, wf.id, ds.id, applyworkflow=JobCreateV2())


if __name__ == "__main__":
    admin = FractalClient()

    working_task = admin.add_working_task()
    failing_task = admin.add_failing_task()

    _user_flow_vanilla(admin, working_task_id=working_task.id)
    _user_flow_power(
        admin, working_task_id=working_task.id, failing_task_id=failing_task.id
    )
    _user_flow_dataset(admin, working_task_id=working_task.id)
    _user_flow_project(admin, working_task_id=working_task.id)
    _user_flow_job(admin, working_task_id=working_task.id)

    admin.wait_for_all_jobs()
