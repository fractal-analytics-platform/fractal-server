import sys

from .utils_pixi import update_pyproject_toml

if __name__ == "__main__":
    if len(sys.argv[1:]) != 3:
        sys.exit(
            "Expect three arguments: path-of-pyproject default-env platform"
        )

    update_pyproject_toml(
        path=sys.argv[1],
        environment=sys.argv[2],
        target_platform=sys.argv[3],
    )


#     from fractal_server.syringe import Inject
# from fractal_server.config import get_settings
# settings = Inject(get_settings)
#         environment=settings.pixi.DEFAULT_ENVIRONMENT,
#         platform=settings.pixi.DEFAULT_PLATFORM,
