from fractal_server.tasks.config._pixi import TasksPixiSettings
from fractal_server.tasks.v2.utils_pixi import get_pixi_home


def test_get_pixi_home(caplog):
    config = TasksPixiSettings(
        default_version="default",
        versions={
            "default": "/common/path/pixi/default/",
            "1.2.3": "/common/path/pixi/1.2.3/",
        },
    ).model_dump()

    home = get_pixi_home(version="1.2.3", config=config, logger_name="abc")
    assert home == "/common/path/pixi/1.2.3/"
    assert caplog.text == ""

    home = get_pixi_home(version="0.0.0", config=config, logger_name="abc")
    assert home == "/common/path/pixi/default/"
    assert "is not available, using default_version" in caplog.text
