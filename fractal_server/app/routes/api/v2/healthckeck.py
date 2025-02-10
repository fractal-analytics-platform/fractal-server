from random import randint

from fastapi import APIRouter
from fastapi import Depends
from fastapi import status

from ....db import AsyncSession
from ....db import get_async_db
from ....models import UserOAuth
from ....schemas.v2 import DatasetCreateV2
from ....schemas.v2 import ProjectCreateV2
from ....schemas.v2 import WorkflowCreateV2
from ...auth import current_active_user
from .dataset import create_dataset
from .project import create_project
from .workflow import create_workflow

router = APIRouter()


@router.post("/", status_code=status.HTTP_200_OK)
async def run_healthcheck(
    user: UserOAuth = Depends(current_active_user),
    db: AsyncSession = Depends(get_async_db),
):
    random_integer = randint(10**9, 10**10 - 1)  # nosec B311

    # Create a project
    project = await create_project(
        project=ProjectCreateV2(name=f"project_{random_integer}"),
        user=user,
        db=db,
    )

    # Create a dataset
    await create_dataset(
        project_id=project.id,
        dataset=DatasetCreateV2(
            name=f"dataset_{random_integer}",
            zarr_dir="/invalid/zarr/dir/not/to/be/used/",
        ),
        user=user,
        db=db,
    )

    # Create a workflow;
    await create_workflow(
        project_id=project.id,
        workflow=WorkflowCreateV2(name=f"workflow_{random_integer}"),
        user=user,
        db=db,
    )
