import json
from typing import Any

from fastapi import FastAPI

from fractal_server.main import start_application


def save_openapi(dest: str = "openapi.json") -> None:
    app: FastAPI = start_application()
    openapi_schema: dict[str, Any] = app.openapi()

    with open(dest, "w") as f:
        json.dump(openapi_schema, f)
