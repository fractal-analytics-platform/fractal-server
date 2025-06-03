from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Depends
from fastapi import Request
from fastapi import Response
from pydantic import BaseModel

from .....logger import set_logger
from ....db import AsyncSession
from ....db import get_async_db
from ....schemas.v2 import TaskGroupActivityV2Read
from fractal_server.app.models import UserOAuth
from fractal_server.app.routes.auth import current_active_verified_user


router = APIRouter()

logger = set_logger(__name__)


class CollectionRequestData(BaseModel):
    pass  # TODO


def parse_request_data():
    pass  # TODO


@router.post(
    "/collect/pixi/",
    response_model=TaskGroupActivityV2Read,
)
async def collect_task_pixi(
    request: Request,
    response: Response,
    background_tasks: BackgroundTasks,
    request_data: CollectionRequestData = Depends(parse_request_data),
    private: bool = False,
    user_group_id: int | None = None,
    user: UserOAuth = Depends(current_active_verified_user),
    db: AsyncSession = Depends(get_async_db),
) -> TaskGroupActivityV2Read:
    pass
