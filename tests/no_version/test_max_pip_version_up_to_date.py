from fractal_server.app.routes.api.v2._aux_functions_task_lifecycle import (
    get_package_version_from_pypi,
)
from fractal_server.config import Settings


async def test_check_pip_latest():
    current_max_pip_version = Settings().FRACTAL_MAX_PIP_VERSION
    pypi_latest_pip_version = await get_package_version_from_pypi("pip")
    hint_msg = (
        "You should update `fractal_server.config.Settings."
        f"FRACTAL_MAX_PIP_VERSION` to '{pypi_latest_pip_version}'"
    )
    assert current_max_pip_version == pypi_latest_pip_version, hint_msg
