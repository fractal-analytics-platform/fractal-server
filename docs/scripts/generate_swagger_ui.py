from pathlib import Path
from textwrap import dedent

from fastapi.openapi.docs import get_swagger_ui_html

# FIXME: Add something for local deployments, pointing somewhere else or nowhere
OPENAPI_URL = "https://fractal-analytics-platform.github.io/fractal-server/openapi/openapi.json"

html_response = get_swagger_ui_html(
    openapi_url=OPENAPI_URL,
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
