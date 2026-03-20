from importlib.metadata import PackageNotFoundError
from importlib.metadata import version

try:
    __version__ = version("mock_pixi_tasks")
except PackageNotFoundError:
    __version__ = "uninstalled"
