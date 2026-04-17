from fastapi.openapi.docs import get_swagger_ui_html

html = get_swagger_ui_html(
    # openapi_url="http://127.0.0.1:8000/openapi.json",
    openapi_url="https://fractal-analytics-platform.github.io/fractal-server/openapi/openapi.json",
    title="Fractal-server API",
)
print(html.body.decode())
