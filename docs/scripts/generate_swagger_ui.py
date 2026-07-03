import os
from pathlib import Path
from textwrap import dedent

from fastapi.openapi.docs import get_swagger_ui_html

openapi_url = os.getenv(
    "OPENAPI_URL", "http://127.0.0.1:8000/fractal-server/openapi.json"
)

html_response = get_swagger_ui_html(
    openapi_url=openapi_url,
    title="Fractal-server API",
)
body = html_response.body.decode()

with (Path(__file__).parent.parent / "openapi.md").open("w") as fp:
    for line in body.splitlines():
        if (
            not line
            or "DOCTYPE" in line
            or "html" in line
            or "head" in line
            or "body" in line
            or "meta" in line
            or "fastapi" in line
            or "title" in line
        ):
            continue
        fp.write(f"{dedent(line)}\n")
