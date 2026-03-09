from fastapi import APIRouter

import fractal_server

router = APIRouter()


@router.get("/alive/")
async def alive():
    return dict(
        alive=True,
        version=fractal_server.__VERSION__,
    )
