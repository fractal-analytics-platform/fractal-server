from fractal_server.app.routes.api.v2._aux_functions_tasks import (
    _verify_non_duplication_group_path,
)


async def test_verify_non_duplication_group_path_none():
    await _verify_non_duplication_group_path(
        path=None,
        resource_id=None,
        db=None,
    )
